{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.ImageTrove.Acid where

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.SafeCopy
import Data.Time.Clock (getCurrentTime, diffUTCTime, secondsToDiffTime, UTCTime(..))
import Data.Typeable
import System.Environment

data LastRunState = LastRunState UTCTime deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''LastRunState)

writeState :: UTCTime -> Update LastRunState ()
writeState newValue = put (LastRunState newValue)

queryState :: Query LastRunState UTCTime
queryState = do LastRunState t <- ask
                return t

$(makeAcidic ''LastRunState ['writeState, 'queryState])

{-
mmain :: IO ()
mmain = do now <- getCurrentTime
           acid <- openLocalState $ LastRunState now
           args <- getArgs
           if null args
           then do string <- query acid QueryState
                   putStrLn $ "The state is: " ++ show string
           else do -- update acid (WriteState (unwords args))
                   -- putStrLn "The state has been modified!"
                   putStrLn "derp"
-}
