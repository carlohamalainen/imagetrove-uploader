{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

-- module Network.ImageTrove.Acid (getLastRunTime, setLastRunTime) where
module Network.ImageTrove.Acid (acidWorker, callWorkerIO, PatientStudySeries(..), AcidAction(..), AcidOutput(..)) where

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.SafeCopy
import Data.Time.LocalTime (ZonedTime(..))
import Data.Typeable

import Control.Concurrent
import Control.Concurrent.STM

import qualified Data.Map as Map

import Control.Exception (onException)
import Control.Concurrent (threadDelay)

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
    loadMap' fp `onException` (print "error: exception in loadMap!" >> threadDelay (1 * 10^6) >> loadMap fp)
  where
    loadMap' fp = do
        acid <- openLocalStateFrom fp (KeyValue Map.empty)
        m <- query acid GetMapInternal
        closeAcidState acid
        return m

updateLastUpdate :: FilePath -> PatientStudySeries -> Maybe ZonedTime -> IO ()
updateLastUpdate fp hashes lastUpdate = do
    updateLastUpdate' fp hashes lastUpdate `onException` (print "error: exception in updateLastUpdate!" >> threadDelay (1 * 10^6) >> updateLastUpdate fp hashes lastUpdate)
  where
    updateLastUpdate' fp hashes lastUpdate = do
        acid <- openLocalStateFrom fp (KeyValue Map.empty)
        _ <- update acid (InsertKey hashes lastUpdate)
        closeAcidState acid

deleteLastUpdate :: FilePath -> PatientStudySeries -> IO ()
deleteLastUpdate fp hashes = do
    deleteLastUpdate' fp hashes `onException` (print "error: exception in deleteLastUpdate!" >> threadDelay (1 * 10^6) >> deleteLastUpdate fp hashes)
  where
    deleteLastUpdate' fp hashes = do
        acid <- openLocalStateFrom fp (KeyValue Map.empty)
        _ <- update acid (DeleteKey hashes)
        closeAcidState acid

data AcidAction = AcidLoadMap FilePath
                | AcidUpdateMap FilePath PatientStudySeries (Maybe ZonedTime)

data AcidOutput = AcidMap (Map.Map Key Value)
                | AcidNothing

acidWorker m = forever $ do
    (action, o) <- takeMVar m

    case action of
        AcidLoadMap fp -> do m <- loadMap fp
                             putMVar o (AcidMap m)

        AcidUpdateMap fp hashes lastUpdate -> do updateLastUpdate fp hashes lastUpdate
                                                 putMVar o AcidNothing

-- FIXME Copied from API.hs
callWorkerIO :: MVar (t, MVar b) -> t -> IO b
callWorkerIO m x = do
    o <- newEmptyMVar
    putMVar m (x, o)
    o' <- takeMVar o
    return o'

-- Stuff to turn into a command line option
_foo fp = do
    m <- loadMap fp
    let m' = Map.toList m

    let someone = filter (\((p,_,_),_) -> p == "4837bc2a-557ab998-ee29fd37-bbe498a8-6b3430cc") m'

    forM_ someone print

    -- Also add option to delete a patient.

    -- *Network.ImageTrove.Acid> m <- loadMap "state_sample_config_files_CAI_7T.conf"
    -- *Network.ImageTrove.Acid> let m' = Map.toList m
    -- *Network.ImageTrove.Acid> let x = filter (\((p,_,_),_) -> p == "HASH OF PATIENT") m'
    -- *Network.ImageTrove.Acid> x
    -- *Network.ImageTrove.Acid> forM_ (map (\(z,_) -> z) x) (deleteLastUpdate "state_sample_config_files_CAI_7T.conf")

