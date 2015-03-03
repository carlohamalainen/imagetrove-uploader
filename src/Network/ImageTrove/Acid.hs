{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

-- module Network.ImageTrove.Acid (getLastRunTime, setLastRunTime) where
module Network.ImageTrove.Acid (loadMap, updateLastUpdate) where

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.SafeCopy
import Data.Time.LocalTime (ZonedTime(..))
import Data.Typeable

import qualified Data.Map as Map

-- Key/Value example copied from acid-state example: https://github.com/acid-state/acid-state/blob/master/examples/KeyValue.hs

type Key   = String
type Value = ZonedTime

data KeyValue = KeyValue !(Map.Map Key Value) deriving (Typeable)

$(deriveSafeCopy 0 'base ''KeyValue)

insertKey :: Key -> Value -> Update KeyValue ()
insertKey key value = do
    KeyValue m <- get
    put (KeyValue (Map.insert key value m))

lookupKey :: Key -> Query KeyValue (Maybe Value)
lookupKey key = do
    KeyValue m <- ask
    return (Map.lookup key m)

getMapInternal :: Query KeyValue (Map.Map Key Value)
getMapInternal = do
    KeyValue m <- ask
    return m

$(makeAcidic ''KeyValue ['insertKey, 'lookupKey, 'getMapInternal])

loadMap :: IO (Map.Map Key Value)
loadMap = do
    acid <- openLocalState (KeyValue Map.empty)
    m <- query acid GetMapInternal
    closeAcidState acid
    return m

updateLastUpdate :: String -> ZonedTime -> IO ()
updateLastUpdate hash lastUpdate = do
    acid <- openLocalState (KeyValue Map.empty)
    _ <- update acid (InsertKey hash lastUpdate)
    closeAcidState acid


{-
getLastRunTime :: IO ZonedTime
getLastRunTime = do
    acid <- openLocalState $ LastRunState time1970
    t <- query acid QueryState
    closeAcidState acid
    return t

setLastRunTime :: ZonedTime -> IO ()
setLastRunTime t = do
    acid <- openLocalState $ LastRunState time1970
    _ <- update acid $ WriteState t
    closeAcidState acid
-}
