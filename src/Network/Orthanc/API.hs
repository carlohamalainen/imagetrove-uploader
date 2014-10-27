{-# LANGUAGE OverloadedStrings, RankNTypes, ScopedTypeVariables #-}

module Network.Orthanc.API where

import Control.Applicative ((<$>))
import Control.Lens
import Control.Monad (join, forM_, when, liftM)

import Control.Monad.Reader
import Control.Monad.Identity
import Control.Monad.Trans (liftIO)

import Data.Aeson (Result(..))
import Data.Either
import Data.Maybe
import Control.Applicative
import Control.Lens
import Control.Exception.Base (catch, IOException(..))
import Control.Monad
import Control.Monad.Identity
import Control.Monad.Reader
import Data.Maybe
import Network.Wreq
import Safe
import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.Map as M

import Data.Aeson
import Data.Aeson.Types
import Data.List (isPrefixOf, isSuffixOf, subsequences)
import Data.Maybe
import Data.Traversable (traverse)
import Network.HTTP.Client (defaultManagerSettings, managerResponseTimeout)
import Network.Mime
import Network.Wreq

import Safe

import Network.MyTardis.Instances

import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.ByteString.Lazy   as BL
import qualified Data.ByteString.Char8  as B8

import qualified Data.Text as T
import qualified Data.Map as M

import System.IO.Temp

import Data.Dicom (createLinksDirectoryFromList, getRecursiveContentsList)
import Network.ImageTrove.Utils (runShellCommand)

opts = defaults & manager .~ Left (defaultManagerSettings { managerResponseTimeout = Just 3000000000 } )

data OrthancPatient = OrthancPatient
    { opID    :: String
    , opIsStable :: Bool
    , opMainDicomTags :: M.Map String String
    , opStudies :: [String]
    , opType :: String
    }
    deriving (Eq, Show)

instance FromJSON OrthancPatient where
    parseJSON (Object v) = OrthancPatient <$>
        v .: "ID"               <*>
        v .: "IsStable"         <*>
        v .: "MainDicomTags"    <*>
        v .: "Studies"          <*>
        v .: "Type"
    parseJSON _          = mzero

data OrthancStudy = OrthancStudy
    { ostudyID :: String
    , ostudyIsStable  :: Bool
    , ostudyMainDicomTags :: M.Map String String
    , ostudyParentPatient :: String
    , ostudySeries :: [String]
    , ostudyType :: String
    }
    deriving (Eq, Show)

instance FromJSON OrthancStudy where
    parseJSON (Object v) = OrthancStudy <$>
        v .: "ID"               <*>
        v .: "IsStable"         <*>
        v .: "MainDicomTags"    <*>
        v .: "ParentPatient"    <*>
        v .: "Series"           <*>
        v .: "Type"
    parseJSON _          = mzero

data OrthancSeries = OrthancSeries
    { oseriesExpectedNumberOfInstances    :: Maybe Integer
    , oseriesID :: String
    , oseriesInstances :: [String]
    , oseriesIsStable :: Bool
    , oseriesMainDicomTags :: M.Map String String
    , oseriesParentStudy :: String
    , oseriesStatus :: String
    , oseriesType :: String
    }
    deriving (Eq, Show)

instance FromJSON OrthancSeries where
    parseJSON (Object v) = OrthancSeries <$>
        v .: "ExpectedNumberOfInstances" <*>
        v .: "ID"                        <*>
        v .: "Instances"                 <*>
        v .: "IsStable"                  <*>
        v .: "MainDicomTags"             <*>
        v .: "ParentStudy"               <*>
        v .: "Status"                    <*>
        v .: "Type"
    parseJSON _          = mzero

data OrthancInstance = OrthancInstance
    { oinstFileSize :: Integer
    , oinstFileUuid :: String
    , oinstID :: String
    , oinstIndexInSeries :: Integer
    , oinstMainDicomTags :: M.Map String String
    , oinstParentSeries :: String
    , oinstType :: String
    }
    deriving (Eq, Show)

instance FromJSON OrthancInstance where
    parseJSON (Object v) = OrthancInstance <$>
        v .: "FileSize" <*>
        v .: "FileUuid" <*>
        v .: "ID" <*>
        v .: "IndexInSeries" <*>
        v .: "MainDicomTags" <*>
        v .: "ParentSeries" <*>
        v .: "Type"
    parseJSON _          = mzero

data Tag = Tag
    { tagName :: String
    , tagType :: String
    , tagValue :: Maybe String
    }
    deriving (Eq, Show)

instance FromJSON Tag where
    parseJSON (Object v) = Tag <$>
        v .: "Name" <*>
        v .: "Type" <*>
        v .: "Value"
    parseJSON _          = mzero

data OrthancTags = OrthancTags
    { otagManufacturer              :: Maybe Tag
    , otagManufacturerModelName     :: Maybe Tag
    , otagReferringPhysicianName    :: Maybe Tag
    , otagStudyDescription          :: Maybe Tag
    , otagSeriesDescription         :: Maybe Tag
    , otagPatientName               :: Maybe Tag
    , otagInstitutionName           :: Maybe Tag
    , otagSequenceName              :: Maybe Tag
    }
    deriving (Eq, Show)

tagSelector :: String -> Either String (OrthancTags -> Maybe String)
tagSelector "Manufacturer"              = Right $ \x -> join $ tagValue <$> otagManufacturer           x
tagSelector "ManufacturerModelName"     = Right $ \x -> join $ tagValue <$> otagManufacturerModelName  x
tagSelector "ReferringPhysicianName"    = Right $ \x -> join $ tagValue <$> otagReferringPhysicianName x
tagSelector "StudyDescription"          = Right $ \x -> join $ tagValue <$> otagStudyDescription       x
tagSelector "SeriesDescription"         = Right $ \x -> join $ tagValue <$> otagSeriesDescription      x
tagSelector "PatientName"               = Right $ \x -> join $ tagValue <$> otagPatientName            x
tagSelector "InstitutionName"           = Right $ \x -> join $ tagValue <$> otagInstitutionName        x
tagSelector "SequenceName"              = Right $ \x -> join $ tagValue <$> otagSequenceName           x
tagSelector x = Left $ "Unknown DICOM field: " ++ show x

instance FromJSON OrthancTags where
    parseJSON (Object v) = OrthancTags <$>
        v .:? "0008,0070" <*>
        v .:? "0008,1090" <*>
        v .:? "0008,0090" <*>
        v .:? "0008,1030" <*>
        v .:? "0008,103e" <*>
        v .:? "0010,0010" <*>
        v .:? "0008,0080" <*>
        v .:? "0018,0024"
    parseJSON _          = mzero

getPatients :: IO (Result [String])
getPatients = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "patients")

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."


getPatient :: String -> IO (Result OrthancPatient)
getPatient oid = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "patients" </> oid)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getStudy :: String -> IO (Result OrthancStudy)
getStudy sid = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "studies" </> sid)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getSeries :: String -> IO (Result OrthancSeries)
getSeries sid = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "series" </> sid)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getInstance :: String -> IO (Result OrthancInstance)
getInstance iid = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "instances" </> iid)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getTags :: String -> IO (Result OrthancTags)
getTags iid = do
    let host = "http://localhost:8042"

    r <- getWith opts (host </> "instances" </> iid </> "tags")

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

hmmStudies :: OrthancPatient -> IO [Result OrthancStudy]
hmmStudies patient = mapM getStudy $ opStudies patient

hmmSeries :: OrthancStudy -> IO [Result OrthancSeries]
hmmSeries study = mapM getSeries $ ostudySeries study

hmmPatients :: IO [Result OrthancPatient]
hmmPatients = flattenResult <$> join (traverse (mapM getPatient) <$> getPatients)

flattenResult :: Result [Result a] -> [Result a]
flattenResult (Success x) = x
flattenResult (Error _) = []

cp :: [a] -> [b] -> [(a, b)]
cp variables values = do
    as <- subsequences variables
    bs <- forM as $ const values
    zip as bs

majorOrthancGroups :: IO [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
majorOrthancGroups = do
    patients <- hmmPatients

    putStrLn $ "majorOrthancGroups: |patients| = " ++ (show $ length patients)

    x <- (flip mapM) patients $ \p -> do studies <- flattenResult <$> traverse hmmStudies p
                                         let patientsAndStudies = cp [p] studies

                                         series <- concat <$> mapM flabert' patientsAndStudies

                                         patientStudySeriesInstance <- concat <$> mapM flabert'' series

                                         patientStudySeriesInstanceTags <- concat <$> mapM flabert''' patientStudySeriesInstance

                                         return patientStudySeriesInstanceTags

    return $ concat x

flabert' (Success patient, Success study) = do
    series <- mapM getSeries (ostudySeries study)
    return [(patient, study, s) | s <- catResults series]

flabert'' (patient, study, series) = do
    oneInstance <- traverse getInstance (headMay $ oseriesInstances series)

    return $ case oneInstance of
        (Just (Success oneInstance')) -> [(patient, study, series, oneInstance')]
        _                             -> error "err1" -- [] -- FIXME Would be better to log something here.

flabert''' (patient, study, series, oneInstance) = do
    tags <- getTags (oinstID oneInstance)
    return $ case tags of Success tags' -> [(patient, study, series, oneInstance, tags')]
                          Error e       -> error e

catResults [] = []
catResults ((Success x):xs) = x:(catResults xs)
catResults ((Error _):xs) = catResults xs

getSeriesArchive :: String -> IO (Either String (FilePath, FilePath))
getSeriesArchive sid = do
    let host = "http://localhost:8042"

    print $ "getSeriesArchive: " ++ sid

    r <- getWith opts (host </> "series" </> sid </> "archive")

    case r ^? responseBody of
        Just body -> catch (do tempDir <- createTempDirectory "/tmp" "dicomOrthancConversion"
                               let zipfile = tempDir </> (sid ++ ".zip")
                               BL.writeFile zipfile body
                               return $ Right (tempDir, zipfile))
                           (\e -> return $ Left $ show (e :: IOException))
        Nothing   -> return $ Left "http error?"

unpackArchive :: FilePath -> FilePath -> IO (Either String FilePath)
unpackArchive tempDir zipfile = catch
    (do setCurrentDirectory tempDir
        unzipResult <- runShellCommand "unzip" ["-q", "-o", zipfile]

        case unzipResult of
            Left e  -> return $ Left e
            Right _ -> do dicomFiles <- filter (not . isSuffixOf ".zip") <$> getRecursiveContentsList tempDir
                          linksDir <- createLinksDirectoryFromList dicomFiles
                          return $ Right linksDir)
    (\e -> return $ Left $ show (e :: IOException))

getOrthancInstrumentGroups
    :: [(String, String)]
    -> [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
    -> Either String [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
getOrthancInstrumentGroups instrumentIdentifiers orthancData =
    if allRights selectors
        then let selectors' = getRights selectors in
                    Right $ filter (\(_, _, _, _, t) -> allTrue selectors' t) orthancData
        else Left $ "Unknown field in instrument selector list: " ++ show instrumentIdentifiers

  where
    allTrue fns x = and [ f x | f <- fns ]

    selectors = map mkSelector instrumentIdentifiers

    mkSelector (fieldName, expectedValue) = case tagSelector fieldName of Right fn -> Right $ \tags -> fn tags == Just expectedValue
                                                                          Left e   -> Left e

    allRights []             = True
    allRights ((Right _):rs) = allRights rs
    allRights ((Left _):_)   = False

    getRights []             = []
    getRights ((Right r):rs) = r:(getRights rs)
    getRights ((Left _):rs)  = getRights rs
