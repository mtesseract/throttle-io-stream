{-# LANGUAGE OverloadedStrings #-}

module Test where

import           Throttle

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as ByteString
import qualified Data.ByteString.Char8    as ByteString.Char8
import           Data.Function
import           Data.Text.Encoding
import           Say

newtype Elem = Elem { unElem :: ByteString } deriving (Show)

measure :: Elem -> Double
measure = fromIntegral . ByteString.length . unElem

produceNext :: TBQueue Elem -> IO (Maybe Elem)
produceNext queue =
  atomically $ Just <$> readTBQueue queue

consumeNext :: Maybe Elem -> IO ()
consumeNext (Just e) = say . decodeUtf8 . unElem $ e
consumeNext Nothing  = say "no more data"

initialDelay :: Int
initialDelay = 5 * 10^6

modifyDelay :: Int -> Int
modifyDelay x = round (0.6 * fromIntegral x)

producer :: TBQueue Elem -> IO ()
producer queue = go 0 initialDelay
  where go n delay = do
          atomically $ writeTBQueue queue (Elem (ByteString.Char8.pack (show n)))
          threadDelay delay
          go (n + 1) (modifyDelay delay)

test :: IO ()
test = do
  let conf = newThrottleConf
        & throttleConfSetMeasure measure
        & throttleConfThrottleProducer
        & throttleConfThrottleConsumer
        & throttleConfSetInterval 1000       -- Interval is one second
        & throttleConfSetMaxThroughput 100   -- 100 Bytes per interval
        & throttleConfSetBufferSize 32

  queue <- atomically $ newTBQueue 1024
  _ <- throttle conf (produceNext queue) consumeNext
  _ <- async (producer queue)

  return ()
