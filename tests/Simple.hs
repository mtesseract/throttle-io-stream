{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent             (threadDelay)
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.Throttle
import           Data.ByteString                (ByteString)
import qualified Data.ByteString                as ByteString
import qualified Data.ByteString.Char8          as ByteString.Char8
import           Data.Function                  ((&))
import           Data.Text.Encoding
import           Say
import           Test.Framework                 (defaultMain, testGroup)
import           Test.Framework.Providers.HUnit (testCase)

newtype Elem = Elem { unElem :: ByteString } deriving (Show)

measure :: Elem -> Double
measure = fromIntegral . ByteString.length . unElem

produceNext :: TBQueue Elem -> IO (Maybe Elem)
produceNext queue = do
  e <- atomically (readTBQueue queue)
  if measure e > 2
     then return Nothing
     else return $ Just e

consumeNext :: Maybe Elem -> IO ()
consumeNext (Just e) = say . decodeUtf8 . unElem $ e
consumeNext Nothing  = return ()

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

simpleTest :: IO ()
simpleTest = do
  let conf = newThrottleConf
        & throttleConfSetMeasure measure
        & throttleConfThrottleProducer
        & throttleConfThrottleConsumer
        & throttleConfSetInterval 1000       -- Interval is one second
        & throttleConfSetMaxThroughput 100   -- 100 Bytes per interval
        & throttleConfSetBufferSize 32

  queue <- atomically $ newTBQueue 1024
  handle <- throttle conf (produceNext queue) consumeNext
  _ <- async (producer queue)
  wait handle

  return ()

main :: IO ()
main = do
  putStrLn ""
  defaultMain tests

  where tests = [ testGroup "Simple"
                  [ testCase "Simple" simpleTest ]
                ]
