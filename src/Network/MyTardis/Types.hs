{-# LANGUAGE OverloadedStrings #-}

module Network.MyTardis.Types where

import qualified Data.Map as M
import qualified Data.MultiMap as MM
import System.FilePath.Posix (takeFileName)

instance (Eq a, Eq b) => Eq (MM.MultiMap a b) where
    m == n = MM.toMap m == MM.toMap n

instance (Show a, Show b) => Show (MM.MultiMap a b) where
    show m = show $ MM.toMap m

-- | An experiment that has been identified on the local filesystem (e.g. a collection
-- of DICOM files).
data IdentifiedExperiment = IdentifiedExperiment
    { ideDescription        :: String   -- ^ Experiment description.
    , ideInstitutionName    :: String   -- ^ Institution name.
    , ideTitle              :: String   -- ^ Experiment title.
    , ideMetadataMaps       :: [(String, MM.MultiMap String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Show)

instance Eq IdentifiedExperiment where
    (IdentifiedExperiment desc1 instName1 title1 _) == (IdentifiedExperiment desc2 instName2 title2 _) = (desc1, instName1, title1) == (desc2, instName2, title2)

-- | A dataset that has been identified on the local filesystem.
data IdentifiedDataset = IdentifiedDataset
    { iddDescription        :: String   -- ^ Dataset description.
    , iddExperiments        :: [String] -- ^ List of experiment resource URIs.
    , iddMetadataMaps       :: [(String, MM.MultiMap String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Eq, Show)

-- | A file that has been identified on the local filesystem.
data IdentifiedFile = IdentifiedFile
    { idfDatasetURL         :: String   -- ^ Resource URI for the dataset.
    , idfFilePath           :: String   -- ^ Full path to the file.
    , idfMd5sum             :: String   -- ^ Md5sum of the file.
    , idfSize               :: Integer  -- ^ Size of the file in bytes.
    , idfMetadataMaps       :: [(String, MM.MultiMap String String)] -- ^ Metadata attribute maps. The first component of the tuple is the schema name (a URL).
    }
    deriving (Show)

-- | Two identified files are considered equal if they are in the same dataset (same 'idfDatasetURL') and
-- have the filename (using 'takeFileName').
instance Eq IdentifiedFile where
    (IdentifiedFile datasetURL filePath _ _ _) == (IdentifiedFile datasetURL' filePath' _ _ _) = datasetURL == datasetURL' && takeFileName filePath == takeFileName filePath'
