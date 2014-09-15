{-# LANGUAGE OverloadedStrings #-}

module Network.MyTardis.Types where

import qualified Data.Map as M
import System.FilePath.Posix (takeFileName)

-- | An experiment that has been identified on the local filesystem (e.g. a collection
-- of DICOM files).
data IdentifiedExperiment = IdentifiedExperiment
    { ideDescription        :: String   -- ^ Experiment description.
    , ideInstitutionName    :: String   -- ^ Institution name.
    , ideTitle              :: String   -- ^ Experiment title.
    , ideMetadataMaps       :: [(String, M.Map String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Eq, Show)

-- | A dataset that has been identified on the local filesystem.
data IdentifiedDataset = IdentifiedDataset
    { iddDescription        :: String   -- ^ Dataset description.
    , iddExperiments        :: [String] -- ^ List of experiment resource URIs.
    , iddMetadataMaps       :: [(String, M.Map String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Eq, Show)

-- | A file that has been identified on the local filesystem.
data IdentifiedFile = IdentifiedFile
    { idfDatasetURL         :: String   -- ^ Resource URI for the dataset.
    , idfFilePath           :: String   -- ^ Full path to the file.
    , idfMd5sum             :: String   -- ^ Md5sum of the file.
    , idfSize               :: Integer  -- ^ Size of the file in bytes.
    , idfMetadataMaps       :: [(String, M.Map String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Show)

-- | Two identified files are considered equal if they are in the same dataset (same 'idfDatasetURL') and
-- have the filename (using 'takeFileName').
instance Eq IdentifiedFile where
    (IdentifiedFile datasetURL filePath _ _ _) == (IdentifiedFile datasetURL' filePath' _ _ _) = datasetURL == datasetURL' && takeFileName filePath == takeFileName filePath'
