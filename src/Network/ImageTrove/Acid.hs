{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

-- module Network.ImageTrove.Acid (getLastRunTime, setLastRunTime) where
module Network.ImageTrove.Acid (loadMap, updateLastUpdate, PatientStudySeries(..)) where

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.SafeCopy
import Data.Time.LocalTime (ZonedTime(..))
import Data.Typeable

import qualified Data.Map as Map

-- Key/Value example copied from acid-state example: https://github.com/acid-state/acid-state/blob/master/examples/KeyValue.hs

type PatientStudySeries = (String, String, String)

type Key   = PatientStudySeries
type Value = Maybe ZonedTime

data KeyValue = KeyValue !(Map.Map Key Value) deriving (Typeable)

$(deriveSafeCopy 0 'base ''KeyValue)

insertKey :: Key -> Value -> Update KeyValue ()
insertKey key value = do
    KeyValue m <- get
    put (KeyValue (Map.insert key value m))

deleteKey :: Key -> Update KeyValue ()
deleteKey key = do
    KeyValue m <- get
    put (KeyValue (Map.delete key m))

lookupKey :: Key -> Query KeyValue (Maybe Value)
lookupKey key = do
    KeyValue m <- ask
    return (Map.lookup key m)

getMapInternal :: Query KeyValue (Map.Map Key Value)
getMapInternal = do
    KeyValue m <- ask
    return m

$(makeAcidic ''KeyValue ['insertKey, 'deleteKey, 'lookupKey, 'getMapInternal])

loadMap :: FilePath -> IO (Map.Map Key Value)
loadMap fp = do
    acid <- openLocalStateFrom fp (KeyValue Map.empty)
    m <- query acid GetMapInternal
    closeAcidState acid
    return m

updateLastUpdate :: FilePath -> PatientStudySeries -> Maybe ZonedTime -> IO ()
updateLastUpdate fp hashes lastUpdate = do
    acid <- openLocalStateFrom fp (KeyValue Map.empty)
    _ <- update acid (InsertKey hashes lastUpdate)
    closeAcidState acid

deleteLastUpdate :: FilePath -> PatientStudySeries -> IO ()
deleteLastUpdate fp hashes = do
    acid <- openLocalStateFrom fp (KeyValue Map.empty)
    _ <- update acid (DeleteKey hashes)
    closeAcidState acid


-- Stuff to turn into a command line option
_foo fp = do
    m <- loadMap fp
    let m' = Map.toList m

    let someone = filter (\((p,_,_),_) -> p == "4837bc2a-557ab998-ee29fd37-bbe498a8-6b3430cc") m'

    forM_ someone print

    -- Also add option to delete a patient.
