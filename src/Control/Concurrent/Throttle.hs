{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- | Throttle

module Control.Concurrent.Throttle
 ( Measure
 , ThrottleConf
 , newThrottleConf
 , throttleConfSetMeasure
 , throttleConfThrottleConsumer
 , throttleConfThrottleProducer
 , throttleConfSetInterval
 , throttleConfSetMaxThroughput
 , throttleConfSetBufferSize
 , throttleConfSetEmaAlpha
 , throttle
 ) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBMQueue
import           Control.Concurrent.Throttle.Ema
import           Control.Monad
import           Control.Monad.IO.Class
import           System.Clock

-- | Type of a measure function for items of the specified type. The
-- measure function is used by the user to specify a notion of size
-- used for throughput computations.
type Measure a = a -> Double

-- | Defines the throttling mode. Consuming and producing can be
-- throttled independently.
data ThrottleMode = ThrottleMode
  { throttleConsumer :: Bool
  , throttleProducer :: Bool
  }

-- | Configuration for throttling.
data ThrottleConf a = ThrottleConf
  { throttleConfMeasure       :: Measure a    -- ^ Measure function for values of type a
  , throttleConfMode          :: ThrottleMode -- ^ Throtting mode
  , throttleConfInterval      :: Double       -- ^ Interval in milliseconds
  , throttleConfMaxThroughput :: Double       -- ^ Maximum throughput allowed
  , throttleConfBufferSize    :: Int          -- ^ Size for buffer queue
  , throttleConfEmaAlpha      :: Double       -- ^ Exponential weight for computing current item size
  }

-- | Type used for internal statistics collection. It is used for
-- approximating the "current" size of items processed.
data Stats = Stats
  { statsEmaItemSizeIn  :: Ema    -- ^ Exponentially weighted moving
                                  -- average for item size of read
                                  -- items
  , statsEmaItemSizeOut :: Ema    -- ^ Exponentially weighted moving
                                  -- average for item size of written
                                  -- items
  } deriving (Show)

-- | Produce a new 'ThrottleConf'.
newThrottleConf :: ThrottleConf a
newThrottleConf = defaultThrottleConf

-- | Default 'ThrottleConf'.
defaultThrottleConf :: ThrottleConf a
defaultThrottleConf = ThrottleConf
  { throttleConfMeasure       = const 1
  , throttleConfMode          = ThrottleMode { throttleConsumer = False
                                             , throttleProducer = False }
  , throttleConfInterval      = 1000
  , throttleConfMaxThroughput = 100
  , throttleConfBufferSize    = 1024
  , throttleConfEmaAlpha      = defaultEmaAlpha }

-- | Set measure function in configuration.
throttleConfSetMeasure :: Measure a -> ThrottleConf a -> ThrottleConf a
throttleConfSetMeasure measure conf = conf { throttleConfMeasure = measure }

throttleConfThrottleProducer :: ThrottleConf a -> ThrottleConf a
throttleConfThrottleProducer conf@ThrottleConf { .. } =
  conf { throttleConfMode = throttleConfMode { throttleProducer = True } }

throttleConfThrottleConsumer :: ThrottleConf a -> ThrottleConf a
throttleConfThrottleConsumer conf@ThrottleConf { .. } =
  conf { throttleConfMode = throttleConfMode { throttleConsumer = True } }

-- | Set interval in configuration.
throttleConfSetInterval :: Double -> ThrottleConf a -> ThrottleConf a
throttleConfSetInterval interval conf = conf { throttleConfInterval = interval }

-- | Set max throughput in configuration.
throttleConfSetMaxThroughput :: Double -> ThrottleConf a -> ThrottleConf a
throttleConfSetMaxThroughput throughput conf =
  conf { throttleConfMaxThroughput = throughput }

-- | Set buffer size in configuration.
throttleConfSetBufferSize :: Int -> ThrottleConf a -> ThrottleConf a
throttleConfSetBufferSize n conf = conf { throttleConfBufferSize = n }

-- | Set exponential weight factor used for computing current item
-- size.
throttleConfSetEmaAlpha :: Double -> ThrottleConf a -> ThrottleConf a
throttleConfSetEmaAlpha alpha conf = conf { throttleConfEmaAlpha = alpha }

-- | Default exponential weight factor for computing current item
-- size.
defaultEmaAlpha :: Double
defaultEmaAlpha = 0.5

-- | Asynchonously read items with the given input callback and write
-- them throttled with the given output callback.
throttle :: ThrottleConf a     -- ^ Throttling configuration
         -> IO (Maybe a)       -- ^ Input callback
         -> (Maybe a -> IO ()) -- ^ Output callback
         -> IO (Async ())      -- ^ Returns an async handler for this
                               -- throttling process
throttle (conf@ThrottleConf { .. }) readItem writeItem = do
  queueBuffer <- atomically $ newTBMQueue throttleConfBufferSize
  statsTVar   <- atomically $ newTVar Stats { statsEmaItemSizeIn  = newEma throttleConfEmaAlpha 0
                                            , statsEmaItemSizeOut = newEma throttleConfEmaAlpha 0 }
  async $
    withAsync (consumer conf statsTVar queueBuffer readItem ) $ \ consumerThread ->
    withAsync (producer conf statsTVar queueBuffer writeItem) $ \ producerThread -> do
    link consumerThread
    link producerThread
    void $ waitBoth consumerThread producerThread

-- | Unthrottled consumer. Fills the provided buffer with new items as
-- fast as possible.
consumerUnthrottled :: TBMQueue a -> IO (Maybe a) -> IO ()
consumerUnthrottled buffer readItem = go
  where go =
          readItem >>= \ case
            Just a  -> atomically (writeTBMQueue buffer a) >> go
            Nothing -> atomically (closeTBMQueue buffer)

-- | Throttled Consumer.
consumerThrottled :: ThrottleConf a
                  -> TVar Stats
                  -> TBMQueue a
                  -> IO (Maybe a)
                  -> IO ()
consumerThrottled (conf@ThrottleConf { .. }) statsTVar buffer readItem = go
  where go = do
          (maybeStats, consumeDuration) <- timeAction $
            readItem >>= \ case
              Just a -> atomically $ do
                writeTBMQueue buffer a
                modifyTVar statsTVar (updateStatsIn conf a)
                Just <$> readTVar statsTVar
              Nothing -> atomically $ do
                closeTBMQueue buffer
                return Nothing
          case maybeStats of
            Just stats -> do
              liftIO . threadDelay $ throttleDelayIn stats conf consumeDuration
              go
            Nothing    -> return ()

-- | Producer thread, dispatches to throttled or non-throttled case.
producer :: ThrottleConf a
         -> TVar Stats
         -> TBMQueue a
         -> (Maybe a -> IO ())
         -> IO ()
producer (conf@ThrottleConf { .. }) stats =
  if throttleProducer throttleConfMode
  then producerThrottled conf stats
  else producerUnthrottled

-- | Consumer, dispatches to throttled or non-throttled case.
consumer :: ThrottleConf a
         -> TVar Stats
         -> TBMQueue a
         -> IO (Maybe a)
         -> IO ()
consumer (conf@ThrottleConf { .. }) stats =
  if throttleConsumer throttleConfMode
  then consumerThrottled conf stats
  else consumerUnthrottled

-- | Throttled Producer. Reads items from the provided buffer and
-- writes them throttled. When the queue is empty, this function
-- returns.
producerThrottled :: ThrottleConf a
                  -> TVar Stats
                  -> TBMQueue a
                  -> (Maybe a -> IO ())
                  -> IO ()
producerThrottled (conf@ThrottleConf { .. }) statsTVar buffer writeItem = go
  where go = do
          (maybeStats, produceDuration) <- timeAction $
            atomically (readTBMQueue buffer) >>= \ case
              Just a -> do
                writeItem (Just a)
                atomically $ do
                  modifyTVar statsTVar (updateStatsOut conf a)
                  Just <$> readTVar statsTVar
              Nothing -> do
                writeItem Nothing
                return Nothing
          case maybeStats of
            Just stats -> do
              liftIO . threadDelay $ throttleDelayOut stats conf produceDuration
              go
            Nothing    -> return ()

-- | Unthrottled producer.
producerUnthrottled :: TBMQueue a
                    -> (Maybe a -> IO ())
                    -> IO ()
producerUnthrottled buffer writeItem = go
  where go =
          atomically (readTBMQueue buffer) >>= \ case
            Just a  -> writeItem (Just a) >> go
            Nothing -> writeItem Nothing

-- | Update provided statistics.
updateStatsOut :: ThrottleConf a -> a -> Stats -> Stats
updateStatsOut ThrottleConf { .. } a (stats@Stats { .. }) =
  stats { statsEmaItemSizeOut = emaUpdate aSize statsEmaItemSizeOut }
  where aSize = throttleConfMeasure a

-- | Update provided statistics.
updateStatsIn :: ThrottleConf a -> a -> Stats -> Stats
updateStatsIn ThrottleConf { .. } a (stats@Stats { .. }) =
  stats { statsEmaItemSizeIn = emaUpdate aSize statsEmaItemSizeIn }
  where aSize = throttleConfMeasure a

-- | Measure execution of an IO action. Returns a pair consisting of
-- the result of the IO action and the duration in milliseconds.
timeAction :: IO a -> IO (a, Double)
timeAction io = do
  t0 <- getTime Monotonic
  a <- io
  t1 <- getTime Monotonic
  return (a, t1 `timeSpecDiff` t0)

  where timeSpecDiff :: TimeSpec -> TimeSpec -> Double
        timeSpecDiff ts1 ts0 =
          fromIntegral (sec ts1 - sec ts0) * 10^3 + fromIntegral (nsec ts1 - nsec ts0) / 10^6

-- | Given a throttle configuration and the current averaged item
-- size, compute the desired delay between two writes.
computeDelay :: ThrottleConf a -> Double -> Double
computeDelay _ 0 = 0
computeDelay ThrottleConf { .. } itemSize =
  throttleConfInterval / (throttleConfMaxThroughput / itemSize)

-- | Delay execution.
throttleDelayIn :: Stats          -- ^ Current throughput statistics
                -> ThrottleConf a -- ^ Throttle configuration
                -> Double         -- ^ Duration of last write
                -> Int            -- ^ Resulting delay in microseconds
throttleDelayIn Stats { .. } = throttleDelay (emaCurrent statsEmaItemSizeIn)

-- | Delay execution.
throttleDelayOut :: Stats          -- ^ Current throughput statistics
                 -> ThrottleConf a -- ^ Throttle configuration
                 -> Double         -- ^ Duration of last write
                 -> Int            -- ^ Resulting delay in microseconds
throttleDelayOut Stats { .. } = throttleDelay (emaCurrent statsEmaItemSizeOut)

throttleDelay :: Double         -- ^ Current item size
              -> ThrottleConf a -- ^ Throttle configuration
              -> Double         -- ^ Duration of last write
              -> Int            -- ^ Resulting delay in microseconds
throttleDelay itemSize (conf@ThrottleConf { .. }) duration =
  round . subtract duration . (10^3 *) $ computeDelay conf itemSize
