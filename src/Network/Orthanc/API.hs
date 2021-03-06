{-# LANGUAGE OverloadedStrings, RankNTypes, ScopedTypeVariables #-}

module Network.Orthanc.API where

import Control.Monad.Trans (liftIO)

import Control.Concurrent (threadDelay)

import Data.Either()

import Control.Applicative
import Control.Lens hiding ((.=))
import Control.Exception.Base (catch, IOException)
import Control.Monad
import Control.Monad.Identity
import Control.Monad.Reader

import qualified Data.Aeson as A
import Data.Aeson.Types((.=))

import Data.Aeson.Lens (key)

import Data.Aeson
import Data.Aeson.Types()
import Data.List (isInfixOf, isSuffixOf, subsequences)
import Data.Maybe()
import Data.Maybe (isNothing, isJust, fromJust)
import Data.Traversable (traverse)
import Network.HTTP.Client (defaultManagerSettings, managerResponseTimeout)
import Network.Mime()
import Network.Wreq hiding (getWith)

import Network.MyTardis.API (writeLog, MyTardisConfig(..))

import Safe

import Network.MyTardis.Instances()

import System.Directory
import System.FilePath
import System.Posix.Files()

import qualified Data.ByteString.Lazy   as BL

import qualified Data.Map as M

import System.IO.Temp

import Data.Dicom (createLinksDirectoryFromList, getRecursiveContentsList)
import Network.ImageTrove.Utils (runShellCommand, getWithE)

import Network.MyTardis.API (allSuccesses)

opts :: Options
opts = defaults & manager .~ Left (defaultManagerSettings { managerResponseTimeout = Just 3000000000 } )

data OrthancPatient = OrthancPatient
    { opID    :: String
    , opIsStable :: Bool
    , opLastUpdate :: String
    , opMainDicomTags :: M.Map String String
    , opStudies :: [String]
    , opType :: String
    , opAnonymizedFrom :: Maybe String
    }
    deriving (Eq, Ord, Show)

instance FromJSON OrthancPatient where
    parseJSON (Object v) = OrthancPatient <$>
        v .: "ID"               <*>
        v .: "IsStable"         <*>
        v .: "LastUpdate"       <*>
        v .: "MainDicomTags"    <*>
        v .: "Studies"          <*>
        v .: "Type"             <*>
        v .:? "AnonymizedFrom"
    parseJSON _          = mzero

data OrthancStudy = OrthancStudy
    { ostudyID :: String
    , ostudyIsStable  :: Bool
    , ostudyLastUpdate :: String
    , ostudyMainDicomTags :: M.Map String String
    , ostudyParentPatient :: String
    , ostudySeries :: [String]
    , ostudyType :: String
    , ostudyAnonymizedFrom :: Maybe String
    }
    deriving (Eq, Show)

instance FromJSON OrthancStudy where
    parseJSON (Object v) = OrthancStudy <$>
        v .: "ID"               <*>
        v .: "IsStable"         <*>
        v .: "LastUpdate"       <*>
        v .: "MainDicomTags"    <*>
        v .: "ParentPatient"    <*>
        v .: "Series"           <*>
        v .: "Type"             <*>
        v .:? "AnonymizedFrom"
    parseJSON _          = mzero

data OrthancSeries = OrthancSeries
    { oseriesID :: String
    , oseriesInstances :: [String]
    , oseriesIsStable :: Bool
    , oseriesLastUpdate :: String
    , oseriesMainDicomTags :: M.Map String String
    , oseriesParentStudy :: String
    , oseriesStatus :: String
    , oseriesType :: String
    , oseriesAnonymizedFrom :: Maybe String
    }
    deriving (Eq, Show)

instance FromJSON OrthancSeries where
    parseJSON (Object v) = OrthancSeries <$>
        v .: "ID"                        <*>
        v .: "Instances"                 <*>
        v .: "IsStable"                  <*>
        v .: "LastUpdate"                <*>
        v .: "MainDicomTags"             <*>
        v .: "ParentStudy"               <*>
        v .: "Status"                    <*>
        v .: "Type"                      <*>
        v .:? "AnonymizedFrom"
    parseJSON _          = mzero

data OrthancInstance = OrthancInstance
    { oinstFileSize :: Integer
    , oinstFileUuid :: String
    , oinstID :: String
    , oinstMainDicomTags :: M.Map String String
    , oinstParentSeries :: String
    , oinstType :: String
    , oinstAnonymizedFrom :: Maybe String
    }
    deriving (Eq, Show)

instance FromJSON OrthancInstance where
    parseJSON (Object v) = OrthancInstance <$>
        v .: "FileSize"                    <*>
        v .: "FileUuid"                    <*>
        v .: "ID"                          <*>
        v .: "MainDicomTags"               <*>
        v .: "ParentSeries"                <*>
        v .: "Type"                        <*>
        v .:? "AnonymizedFrom"
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
    , otagStationName               :: Maybe Tag
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
tagSelector "StationName"               = Right $ \x -> join $ tagValue <$> otagStationName            x
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
        v .:? "0018,0024" <*>
        v .:? "0008,1010"
    parseJSON _          = mzero

askHost :: ReaderT MyTardisConfig IO String
askHost = orthancHost <$> ask

getPatients :: ReaderT MyTardisConfig IO (Result [String])
getPatients = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "patients")

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                        Just v      -> fromJSON v
                        Nothing     -> Error "Could not decode resource."

getPatient :: String -> ReaderT MyTardisConfig IO (Result OrthancPatient)
getPatient oid = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "patients" </> oid)

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                       Just v  -> let p = fromJSON v in
                                        case opIsStable <$> p of
                                           Success True    -> p
                                           Success False   -> Error $ "Patient " ++ oid ++ " is not stable in Orthanc."
                                           Error   e       -> Error $ "Could not get patient " ++ oid ++ ", error: " ++ e
                       Nothing -> Error $ "Could not decode resource for patient " ++ oid

getStudy :: String -> ReaderT MyTardisConfig IO (Result OrthancStudy)
getStudy sid = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "studies" </> sid)

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                       Just v      -> let s = fromJSON v in
                                        case ostudyIsStable <$> s of
                                           Success True    -> s
                                           Success False   -> Error $ "Study " ++ sid ++ " is not stable in Orthanc."
                                           Error   e       -> Error $ "Could not get study " ++ sid ++ ", error: " ++ e
                       Nothing     -> Error $ "Could not decode resource for study " ++ sid

getSeries :: String -> ReaderT MyTardisConfig IO (Result OrthancSeries)
getSeries sid = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "series" </> sid)

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                       Just v      -> let s = fromJSON v in
                                        case oseriesIsStable <$> s of
                                           Success True    -> s
                                           Success False   -> Error $ "Series " ++ sid ++ " is not stable in Orthanc."
                                           Error   e       -> Error $ "Could not get series " ++ sid ++ ", error: " ++ e
                       Nothing     -> Error $ "Could not decode resource for study " ++ sid

getInstance :: String -> ReaderT MyTardisConfig IO (Result OrthancInstance)
getInstance iid = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "instances" </> iid)

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                       Just v      -> fromJSON v
                       Nothing     -> Error $ "Could not decode resource for instance " ++ iid

getTags :: String -> ReaderT MyTardisConfig IO (Result OrthancTags)
getTags iid = do
    host <- askHost

    r <- liftIO $ getWithE opts (host </> "instances" </> iid </> "tags")

    return $ case r of
        Left e   -> Error e
        Right r' -> case (join $ decode <$> r' ^? responseBody :: Maybe Value) of
                        Just v      -> fromJSON v
                        Nothing     -> Error $ "Could not decode resource for instance " ++ iid

getPatientsStudies :: OrthancPatient -> ReaderT MyTardisConfig IO [Result OrthancStudy]
getPatientsStudies patient = mapM getStudy $ opStudies patient

{-
getStudysSeries :: OrthancStudy -> ReaderT MyTardisConfig IO [Result OrthancSeries]
getStudysSeries study = mapM getSeries $ ostudySeries study
-}

-- Gets all real (not anonymized) patients.
getAllPatients :: ReaderT MyTardisConfig IO [Result OrthancPatient]
getAllPatients = flattenResult <$> join (traverse (mapM getPatient) <$> getPatients)

flattenResult :: Result [Result a] -> [Result a]
flattenResult (Success x) = x
flattenResult (Error _) = []

crossProduct :: [a] -> [b] -> [(a, b)]
crossProduct variables values = do
    as <- subsequences variables
    bs <- forM as $ const values
    zip as bs

majorOrthancGroups :: ReaderT MyTardisConfig IO [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
majorOrthancGroups = do
    patients <- getAllPatients

    writeLog $ "majorOrthancGroups: |patients| = " ++ show (length patients)

    x <- forM patients $ \p -> do studies <- flattenResult <$> traverse getPatientsStudies p
                                  let patientsAndStudies = crossProduct [p] studies

                                  series <- concat <$> mapM extendWithSeries patientsAndStudies

                                  patientStudySeriesInstance <- concat <$> mapM extendWithOneInstance series

                                  -- patient, study, series, instance, tags
                                  concat <$> mapM extendWithTags patientStudySeriesInstance

    return $ concat x

isSuccessTrue (Success True) = True
isSuccessTrue _              = False

extendWithSeries (Success patient, Success study) = do
    _series <- mapM getSeries (ostudySeries study)
    let series = filter (\s -> isSuccessTrue $ fmap (isNothing . oseriesAnonymizedFrom) s) _series
    return [(patient, study, s) | s <- catResults series]
extendWithSeries _ = return []

extendWithOneInstance (patient, study, series) = do
    oneInstance <- traverse getInstance (headMay $ oseriesInstances series)

    case oneInstance of
        (Just (Success oneInstance')) -> return [(patient, study, series, oneInstance')]
        (Just (Error e))              -> do writeLog $ "Error while retrieving single instance: " ++ e
                                            writeLog $ "We were trying to retrieve: " ++ show (patient, study, series)
                                            return []
        (Nothing)                     -> do writeLog $ "Warning: got Nothing when trying to retrieve single instance of series: " ++ show series
                                            writeLog $ "We were trying to retrieve: " ++ show (patient, study, series)
                                            return []

extendWithTags (patient, study, series, oneInstance) = do
    tags <- getTags (oinstID oneInstance)
    case tags of Success tags' -> return [(patient, study, series, oneInstance, tags')]
                 Error e       -> do writeLog $ "Warning: could not retrieve tags for instance: " ++ e
                                     return []

-- FIXME This is silly - throwing away possible errors. Also the
-- type of extendWithSeries should be different.
catResults [] = []
catResults (Success x:xs) = x : catResults xs
catResults (Error _:xs) = catResults xs

anonymizeSeries :: String -> ReaderT MyTardisConfig IO (Either String String)
anonymizeSeries = undefined

getSeriesArchive :: String -> ReaderT MyTardisConfig IO (Either String (FilePath, FilePath))
getSeriesArchive sid = do
    host <- askHost

    tmp <- mytardisTmp <$> ask

    tempDir <- liftIO $ createTempDirectory tmp "dicomOrthancConversion"
    let zipfile = tempDir </> (sid ++ ".zip")

    let url = host </> "series" </> sid </> "archive"

    let cmd = "wget"
    let args = ["-q", url, "-O", zipfile]

    liftIO $ print ("getSeriesArchive", cmd, args)

    getZipResult <- liftIO $ runShellCommand (dropFileName zipfile) cmd args

    liftIO $ print ("getSeriesArchive", getZipResult)

    case getZipResult of
        Left e  -> return $ Left e
        Right _ -> do liftIO $ putStrLn $ "getSeriesArchive: successfully downloaded " ++ zipfile
                      return $ Right (tempDir, zipfile)

    {-
    writeLog $ "getSeriesArchive 1: " ++ sid

    r <- liftIO $ getWith opts (host </> "series" </> sid </> "archive")

    writeLog $ "getSeriesArchive 2: " ++ sid ++ " finished."


    case r ^? responseBody of
        Just body -> liftIO $ catch (do tempDir <- createTempDirectory tmp "dicomOrthancConversion"
                                        let zipfile = tempDir </> (sid ++ ".zip")
                                        BL.writeFile zipfile body
                                        putStrLn $ "getSeriesArchive 4: write zip file: " ++ zipfile
                                        return $ Right (tempDir, zipfile))
                                    (\e -> do putStrLn $ "getSeriesArchive 5: error, IO exception: " ++ show (e :: IOException)
                                              return $ Left $ show (e :: IOException))
        Nothing   -> return $ Left $ "Error: getSeriesArchive 6: empty response body in getSeriesArchive for series " ++ sid
    -}


unpackArchive :: FilePath -> FilePath -> FilePath -> IO (Either String FilePath)
unpackArchive tmp tempDir zipfile = catch
    (do unzipResult <- runShellCommand tempDir "unzip" ["-q", "-o", zipfile]

        case unzipResult of
            Left e  -> return $ Left e
            Right _ -> do dicomFiles <- filter (not . isSuffixOf ".zip") <$> getRecursiveContentsList tempDir
                          linksDir <- createLinksDirectoryFromList tmp dicomFiles
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

    allRights []           = True
    allRights (Right _:rs) = allRights rs
    allRights (Left _:_)   = False

    getRights []           = []
    getRights (Right r:rs) = r : getRights rs
    getRights (Left _:rs)  = getRights rs
