-- | Exponentially weighted moving average

module Control.Concurrent.Throttle.Ema
  ( Ema
  , emaCurrent
  , emaWeight
  , displayEma
  , displayEmaDefault
  , newEma
  , emaModify
  , emaUpdate
  ) where

import           Data.Text (Text)
import qualified Data.Text as Text (pack)

data Ema = Ema { emaWeight :: !Double, emaCurrent :: !Double } deriving (Show)

newEma :: Double -> Double -> Ema
newEma α initialValue  = Ema { emaWeight = α, emaCurrent = initialValue }

emaModify :: (Double -> Double -> Double) -> Ema -> Ema
emaModify f ema = ema { emaCurrent = f (emaWeight ema) (emaCurrent ema) }

emaUpdate :: Double -> Ema -> Ema
emaUpdate newSample = emaModify (\ α current -> α * newSample + (1 - α) * current)

round' :: Int -> Double -> Double
round' nDigits = (/ 10^nDigits) . fromIntegral . round . (* 10^nDigits)

defaultDisplayPrecision :: Int
defaultDisplayPrecision = 1

displayEma :: Int -> Ema -> Text
displayEma precision = Text.pack . show . round' precision . emaCurrent

displayEmaDefault :: Ema -> Text
displayEmaDefault = displayEma defaultDisplayPrecision
