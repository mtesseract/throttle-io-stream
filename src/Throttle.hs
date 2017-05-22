{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- | Throttle

module Throttle
 ( Measure
 , ThrottleConf
 , newThrottleConf
 , throttleConfSetMeasure
 , throttleConfSetInterval
 , throttleConfSetMaxThroughput
 , throttleConfSetBufferSize
 , throttleConfSetEmaAlpha
 , throttle
 ) where

import           Control.Arrow
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBMQueue
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Function
import           Ema
import           System.Clock

type Measure a = a -> Double

data ThrottleConf a = ThrottleConf
  { throttleConfMeasure       :: Measure a -- ^ Measure function for values of type a
  , throttleConfInterval      :: Double    -- ^ Interval in milliseconds
  , throttleConfMaxThroughput :: Double    -- ^ Maximum throughput allowed
  , throttleConfBufferSize    :: Int       -- ^ Size for buffer queue
  , throttleConfEmaAlpha      :: Double    -- ^ Exponential weight for computing current item size
  }

newtype Stats = Stats
  { statsEmaItemSize :: Ema    -- ^ Exponentially weighted moving average for item size
  } deriving (Show)

-- | Produce a new 'ThrottleConf'.
newThrottleConf :: ThrottleConf a
newThrottleConf = defaultThrottleConf

-- | Default 'ThrottleConf'.
defaultThrottleConf :: ThrottleConf a
defaultThrottleConf = ThrottleConf { throttleConfMeasure       = const 1
                                   , throttleConfInterval      = 1000
                                   , throttleConfMaxThroughput = 100
                                   , throttleConfBufferSize    = 1024
                                   , throttleConfEmaAlpha      = defaultEmaAlpha
                                   }

-- | Set measure function in configuration.
throttleConfSetMeasure :: Measure a -> ThrottleConf a -> ThrottleConf a
throttleConfSetMeasure measure conf = conf { throttleConfMeasure = measure }

-- | Set interval in configuration.
throttleConfSetInterval :: Double -> ThrottleConf a -> ThrottleConf a
throttleConfSetInterval interval conf = conf { throttleConfInterval = interval }

-- | Set max throughput in configuration.
throttleConfSetMaxThroughput :: Double -> ThrottleConf a -> ThrottleConf a
throttleConfSetMaxThroughput throughput conf = conf { throttleConfMaxThroughput = throughput }

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
throttle :: ThrottleConf a  -- ^ Throttling configuration
         -> IO (Maybe a)    -- ^ Input callback
         -> (a -> IO ())    -- ^ Output callback
         -> IO (Async ())   -- ^ Returns an async handler for this throttling process
throttle (conf @ ThrottleConf { .. }) readItem writeItem = do
  queueBuffer <- atomically $ newTBMQueue throttleConfBufferSize
  statsTVar   <- atomically $ newTVar Stats { statsEmaItemSize = newEma throttleConfEmaAlpha 0 }
  async $
    withAsync (consumer conf queueBuffer statsTVar readItem ) $ \ consumerThread ->
    withAsync (producer conf queueBuffer statsTVar writeItem) $ \ producerThread -> do
    link consumerThread
    link producerThread
    void $ waitAny [consumerThread, producerThread]

-- | Consumer thread. Fills the provided buffer with new items as fast
-- as possible.
consumer :: ThrottleConf a -> TBMQueue a -> TVar Stats -> IO (Maybe a) -> IO ()
consumer _ buffer _ readItem = go
  where go =
          readItem >>= \ case
            Just a  -> atomically (writeTBMQueue buffer a) >> go
            Nothing -> atomically (closeTBMQueue buffer)

-- | Producer thread. Reads items from the provided buffer and writes
-- them delayed. When the queue is empty, this function returns.
producer :: ThrottleConf a -> TBMQueue a -> TVar Stats -> (a -> IO ()) -> IO ()
producer (conf @ ThrottleConf { .. }) buffer statsTVar writeItem = go
  where go =
          atomically (readTBMQueue buffer) >>= \ case
            Just a -> do
              -- Measure duration of writing the next item.
              (stats, writeDuration) <- timeAction $ do
                writeItem a
                atomically $ do
                  modifyTVar statsTVar (updateStats conf a)
                  readTVar statsTVar
              publishDelayIO conf writeDuration stats
              go
            Nothing ->
              return ()

-- | Update provided statistics.
updateStats :: ThrottleConf a -> a -> Stats -> Stats
updateStats ThrottleConf { .. } a stats@Stats { .. } =
  stats { statsEmaItemSize = emaUpdate aSize statsEmaItemSize }
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
publishDelayIO :: ThrottleConf a -- ^ Throttle configuration
               -> Double         -- ^ Duration of last write
               -> Stats          -- ^ Current throughput statistics
               -> IO ()          -- ^ Resulting delay in milliseconds
publishDelayIO (conf @ ThrottleConf { .. }) writeDuration Stats { .. } =
  computeDelay conf (emaCurrent statsEmaItemSize)
    & ((* 10^3)
       >>> subtract writeDuration
       >>> round
       >>> threadDelay
       >>> liftIO)
