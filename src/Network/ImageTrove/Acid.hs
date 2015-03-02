{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.ImageTrove.Acid (getLastRunTime, setLastRunTime) where

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.SafeCopy
import Data.Time.Clock (getCurrentTime, diffUTCTime, secondsToDiffTime, UTCTime(..))
import Data.Time.LocalTime (ZonedTime(..))
import Data.Time.Format (parseTime)
import Data.Typeable
import System.Environment
import System.Locale (defaultTimeLocale)

data LastRunState = LastRunState ZonedTime deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''LastRunState)

writeState :: ZonedTime -> Update LastRunState ()
writeState newValue = put (LastRunState newValue)

queryState :: Query LastRunState ZonedTime
queryState = do LastRunState t <- ask
                return t

$(makeAcidic ''LastRunState ['writeState, 'queryState])

time1970 :: ZonedTime
Just time1970 = parseTime defaultTimeLocale "%Y%m%dT%H%M%S" "19700101000000"

getLastRunTime :: IO ZonedTime
getLastRunTime = do
    acid <- openLocalState $ LastRunState time1970
    query acid QueryState

setLastRunTime :: ZonedTime -> IO ()
setLastRunTime t = do
    acid <- openLocalState $ LastRunState time1970
    update acid $ WriteState t
