# throttle-io-stream [![Hackage version](https://img.shields.io/hackage/v/throttle-io-stream.svg?label=Hackage)](https://hackage.haskell.org/package/throttle-io-stream) [![Stackage version](https://www.stackage.org/package/throttle-io-stream/badge/lts?label=Stackage)](https://www.stackage.org/package/throttle-io-stream) [![Build Status](https://travis-ci.org/mtesseract/throttle-io-stream.svg?branch=master)](https://travis-ci.org/mtesseract/throttle-io-stream)

### About

This packages provides throttling functionality for arbitrary IO
producers and consumers. The core function exported is the following:

```haskell
throttle :: ThrottleConf a     -- ^ Throttling configuration
         -> IO (Maybe a)       -- ^ Input callback
         -> (Maybe a -> IO ()) -- ^ Output callback
         -> IO (Async ())      -- ^ Returns an async handler for this
                               -- throttling process
```

This will spawn asynchronous operations that

1. consume data using the provided input callback and write it into an
  internal buffering queue and

1. produce data from the buffering queue using the provided consumer
   callback.
