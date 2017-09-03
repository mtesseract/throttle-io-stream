-- | Exponentially weighted moving average

{-# LANGUAGE RecordWildCards #-}

module Control.Concurrent.Throttle.Ema
  ( Ema
  , emaCurrent
  , emaWeight
  , newEma
  , emaUpdate
  ) where

data Ema = Ema { emaWeight  :: !Double
               , emaCurrent :: !Double
               } deriving (Show)

newEma :: Double -> Double -> Ema
newEma α initialValue  = Ema { emaWeight = α, emaCurrent = initialValue }

emaModifyCurrent :: (Ema -> Double) -> Ema -> Ema
emaModifyCurrent f ema = ema { emaCurrent = f ema }

emaUpdate :: Double -> Ema -> Ema
emaUpdate newSample = emaModifyCurrent $
  \ Ema { .. } -> emaWeight * newSample + (1 - emaWeight) * emaCurrent
