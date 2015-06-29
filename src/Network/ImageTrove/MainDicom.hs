{-# LANGUAGE DeriveDataTypeable, OverloadedStrings #-}

module Network.ImageTrove.MainDicom where

import Prelude hiding (lookup)

import Control.Exception (IOException(..))

import Control.Monad.Catch
import Control.Monad.Reader
import Control.Monad (forever, when, mapM)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, poll, mapConcurrently)
import Data.Char (toLower, isAlphaNum)
import Data.Configurator
import Data.Configurator.Types
import Data.Monoid (mempty)
import Data.Traversable (traverse)
import Data.Typeable
import Data.Either
import qualified Data.Foldable as DF
import Data.Maybe
import Data.List (groupBy, sortBy, isPrefixOf, intercalate)
import Data.Ord (comparing)
import qualified Data.Map as M
import qualified Data.Aeson as A
import Data.Aeson (Result(..))
import qualified Data.Text as T
import Options.Applicative
import Safe (headMay)
import Text.Printf (printf)

import qualified Data.Set as S

import Data.List.Split (splitEvery)

import Data.Dicom
import Network.ImageTrove.Utils
import Network.MyTardis.API
import Network.MyTardis.RestTypes
import Network.MyTardis.Types
import Network.Orthanc.API

import System.IO

import Control.Concurrent (threadDelay)

import System.Unix.Directory (removeRecursiveSafely)

import Data.Time.Clock (addUTCTime, diffUTCTime, UTCTime(..), NominalDiffTime(..))
import Data.Time.LocalTime (getZonedTime, utcToZonedTime, zonedTimeToUTC, TimeZone(..), ZonedTime(..))
import Data.Time.Format (parseTime)
import System.Locale (defaultTimeLocale)

import System.Directory (copyFile, createDirectory, getCurrentDirectory, setCurrentDirectory, createDirectoryIfMissing)
import System.FilePath (makeValid, (</>))

import Network.ImageTrove.Acid

import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.Map as Map

import Control.Concurrent.ParallelIO.Local

import Control.Concurrent
import Control.Concurrent.STM

import Network.Wreq hiding (header)
-- import Control.Exception.Base (catch, IOException)
import Control.Lens hiding ((.=))
import System.IO.Temp
import qualified Data.ByteString.Lazy   as BL

import System.FilePath

import qualified Data.Conduit      as DC
import qualified Data.Conduit.List as CL


data Command
    = CmdUploadAll              UploadAllOptions
    | CmdUploadOne              UploadOneOptions
    | CmdShowExperiments        ShowExperimentsOptions
    | CmdUploadFromDicomServer  UploadFromDicomServerOptions
    | CmdShowNewExperiments     ShowNewExperimentsOptions
    deriving (Eq, Show)

data ShowNewExperimentsOptions = ShowNewExperimentsOptions deriving (Eq, Show)

data UploadAllOptions = UploadAllOptions { uploadAllDryRun :: Bool } deriving (Eq, Show)

data UploadOneOptions = UploadOneOptions { uploadOneHash :: String } deriving (Eq, Show)

data ShowExperimentsOptions = ShowExperimentsOptions { showFileSets :: Bool } deriving (Eq, Show)

data UploadFromDicomServerOptions = UploadFromDicomServerOptions { uploadFromDicomForever       :: Bool
                                                                 , uploadFromDicomSleepMinutes  :: Int
                                                                 } deriving (Eq, Show)

data UploaderOptions = UploaderOptions
    { optDirectory      :: Maybe FilePath
    , optHost           :: Maybe String
    , optConfigFile     :: FilePath
    , optDebug          :: Bool
    , optCommand        :: Command
    }
    deriving (Eq, Show)

pShowNewExperimentsOption :: Parser Command
pShowNewExperimentsOption = CmdShowNewExperiments <$> (pure ShowNewExperimentsOptions)

pUploadAllOptions :: Parser Command
pUploadAllOptions = CmdUploadAll <$> UploadAllOptions <$> switch (long "dry-run" <> help "Dry run.")

pUploadOneOptions :: Parser Command
pUploadOneOptions = CmdUploadOne <$> UploadOneOptions <$> strOption (long "hash" <> help "Hash of experiment to upload.")

pShowExprOptions :: Parser Command
pShowExprOptions = CmdShowExperiments <$> ShowExperimentsOptions <$> switch (long "show-file-sets" <> help "Show experiments.")

pUploadFromDicomServerOptions :: Parser Command
pUploadFromDicomServerOptions = CmdUploadFromDicomServer <$> (UploadFromDicomServerOptions <$> switch (long "upload-forever" <> help "Run forever with sleep between uploads.")
                                                                                           <*> option auto (long "sleep-minutes"  <> help "Number of minutes to sleep between uploads."))

pUploaderOptions :: Parser UploaderOptions
pUploaderOptions = UploaderOptions
    <$> optional (strOption (long "input-dir"     <> metavar "DIRECTORY" <> help "Directory with DICOM files."))
    <*> optional (strOption (long "host"          <> metavar "HOST"      <> help "MyTARDIS host URL, e.g. http://localhost:8000"))
    <*>          (strOption (long "config"        <> metavar "CONFIG"    <> help "Configuration file."))
    <*>          (switch    (long "debug"                                <> help "Debug mode."))
    <*> subparser x
  where
    -- x    = cmd1 <> cmd2 <> cmd3 <> cmd4
    -- FIXME For the moment just do DICOM server stuff.
    x    = cmd4 <> cmd5
    cmd1 = command "upload-all"               (info (helper <*> pUploadAllOptions) (progDesc "Upload all experiments."))
    cmd2 = command "upload-one"               (info (helper <*> pUploadOneOptions) (progDesc "Upload a single experiment."))
    cmd3 = command "show-experiments"         (info (helper <*> pShowExprOptions)  (progDesc "Show local experiments."))
    cmd4 = command "upload-from-dicom-server" (info (helper <*> pUploadFromDicomServerOptions) (progDesc "Upload from DICOM server."))
    cmd5 = command "show-new"                 (info (helper <*> pShowNewExperimentsOption)     (progDesc "Show new (unprocessed) patients on DICOM server."))

getDicomDir :: UploaderOptions -> FilePath
getDicomDir opts = fromMaybe "." (optDirectory opts)

hashFiles :: [FilePath] -> String
hashFiles = sha256 . unwords

createProjectGroup :: MVar (String, MVar (A.Result RestGroup)) -> FilePath -> ReaderT MyTardisConfig IO (A.Result RestGroup)
createProjectGroup groupMVar linksDir = do
    projectID <- liftIO $ caiProjectID <$> rights <$> (getDicomFilesInDirectory ".IMA" linksDir >>= mapM readDicomMetadata)
    _createProjectGroup groupMVar projectID

_createProjectGroup groupMVar projectID = do
    case projectID of A.Success projectID' -> do projectResult <- callWorker groupMVar $ "Project " ++ projectID'
                                                 case projectResult of A.Success _              -> liftIO $ putStrLn $ "Created project group: " ++ projectID'
                                                                       A.Error   projErr        -> liftIO $ putStrLn $ "Error when creating project group: " ++ projErr
                                                 return (projectResult :: A.Result RestGroup)
                      A.Error   err        -> do liftIO $ putStrLn $ "Error: could not retrieve Project ID from ReferringPhysician field: " ++ err
                                                 return $ A.Error err

uploadAllAction opts = do
    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \(instrumentName, instrumentFilters, instrumentFiltersT, instrumentMetadataFields, experimentFields, datasetFields, schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress, defaultOperators) -> uploadDicomAsMinc instrumentFilters instrumentMetadataFields experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile (getDicomDir opts) (schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress, defaultOperators)

anonymizeDicomFile :: FilePath -> IO (Either String String)
anonymizeDicomFile f = do
    patientID <- (fmap dicomPatientID) <$> (readDicomMetadata f)

    case patientID of
        Left e                  -> return $ Left e
        Right Nothing           -> return $ Left $ "No PatientID in " ++ f
        Right (Just patientID') -> do let cmd = "dcmodify"
                                          opts = [ "-ie"
                                                 , "-gin"
                                                 , "-nb"
                                                 , "-imt"
                                                 , "-ma", "(0010,0010)=" ++ patientID' -- Patient Name = Patient ID
                                                 , "-ea", "(0010,0030)" -- Patient birth date
                                                 , "-ea", "(0008,0050)" -- Accession number
                                                 , "-ea", "(0020,000D)" -- Study Instance UID
                                                 , "-ea", "(0020,000E)" -- Series Instance UID
                                                 , "-ea", "(0008,0018)" -- SOP Instance UID
                                                 , "-ea", "(0008,0080)" -- Institution Name
                                                 , "-ea", "(0008,0081)" -- Institution Address
                                                 , "-ea", "(0008,1070)" -- Operator Name
                                                 , "-ea", "(0008,1155)" -- Referenced SOP Instance UID
                                                 , "-ea", "(0010,1000)" -- Other Patient Ids
                                                 , "-ea", "(0020,0010)" -- Study ID
                                                 , "-ea", "(0020,4000)" -- Image Comments
                                                 , f
                                                 ]

                                      putStrLn $ "anonymizeDicomFile: " ++ show (cmd, opts)

                                      runShellCommand (dropFileName f) cmd opts

anonymizeDicomFile' :: FilePath -> IO String
anonymizeDicomFile' f = do
    patientID <- (fmap dicomPatientID) <$> (readDicomMetadata f)

    case patientID of
        Left e                  -> throwM $ AnonymizationException e
        Right Nothing           -> throwM $ AnonymizationException $ "No PatientID in " ++ f
        Right (Just patientID') -> do let cmd = "dcmodify"
                                          opts = [ "-ie"
                                                 , "-gin"
                                                 , "-nb"
                                                 , "-imt"
                                                 , "-ma", "(0010,0010)=" ++ patientID' -- Patient Name = Patient ID
                                                 , "-ea", "(0010,0030)" -- Patient birth date
                                                 , "-ea", "(0008,0050)" -- Accession number
                                                 , "-ea", "(0020,000D)" -- Study Instance UID
                                                 , "-ea", "(0020,000E)" -- Series Instance UID
                                                 , "-ea", "(0008,0018)" -- SOP Instance UID
                                                 , "-ea", "(0008,0080)" -- Institution Name
                                                 , "-ea", "(0008,0081)" -- Institution Address
                                                 , "-ea", "(0008,1070)" -- Operator Name
                                                 , "-ea", "(0008,1155)" -- Referenced SOP Instance UID
                                                 , "-ea", "(0010,1000)" -- Other Patient Ids
                                                 , "-ea", "(0020,0010)" -- Study ID
                                                 , "-ea", "(0020,4000)" -- Image Comments
                                                 , f
                                                 ]

                                      putStrLn $ "anonymizeDicomFile: " ++ show (cmd, opts)

                                      _runShellCommand (dropFileName f) cmd opts

uploadDicomAction opts origDir = do
    liftIO $ print "uploadDicomAction: entering."
{-
    debug <- mytardisDebug <$> ask

    cwd <- liftIO getCurrentDirectory

    let slashToUnderscore = map (\c -> if c == '/' then '_' else c)

    let fp = cwd </> (slashToUnderscore $ "state_" ++ optConfigFile opts)
    liftIO $ createDirectoryIfMissing True fp

    conf <- ask

    acidMVar       <- liftIO $ newEmptyMVar
    experimentMVar <- liftIO $ newEmptyMVar
    datasetMVar    <- liftIO $ newEmptyMVar
    groupMVar      <- liftIO $ newEmptyMVar

    -- FIXME Would be nicer if we could avoid the runReaderT stuff here?
    asyncAcidWorker             <- liftIO $ async $ acidWorker acidMVar
    asyncWorkerCreateExperiment <- liftIO $ async $ runReaderT (workerCreateExperiment experimentMVar) conf
    asyncWorkerCreateDataset    <- liftIO $ async $ runReaderT (workerCreateDataset    datasetMVar)    conf
    asyncWorkerCreateGroup      <- liftIO $ async $ runReaderT (workerCreateGroup      groupMVar)      conf

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)
    tasks <- concat <$> forM instrumentConfigs (makeTasks conf debug fp acidMVar experimentMVar datasetMVar groupMVar)

    liftIO $ putStrLn $ "uploadDicomAction: found " ++ show (length tasks) ++ " tasks to process."

    -- FIXME Add an ID to the config so that logging has a task ID.
    liftIO $ withPool 15 $ \pool -> parallel_ pool tasks

    pollExperiment <- liftIO $ poll asyncWorkerCreateExperiment
    pollDataset    <- liftIO $ poll asyncWorkerCreateDataset
    pollGroup      <- liftIO $ poll asyncWorkerCreateGroup
    pollAcid       <- liftIO $ poll asyncAcidWorker

    liftIO $ putStrLn $ "poll pollExperiment: " ++ show pollExperiment
    liftIO $ putStrLn $ "poll pollDataset: " ++ show pollDataset
    liftIO $ putStrLn $ "poll pollGroup: " ++ show (pollGroup :: Maybe (Either SomeException ()))
    liftIO $ putStrLn $ "poll pollAcid: " ++ show (pollAcid :: Maybe (Either SomeException ()))

    liftIO $ print "uploadDicomAction: exiting."
-}



{-
makeTasks conf debug fp acidMVar experimentMVar datasetMVar groupMVar iconfig = do
    let ( instrumentName, instrumentFilters, instrumentFiltersT, instrumentMetadataFields, experimentFields, datasetFields, schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress, defaultOperators) = iconfig

    liftIO $ putStrLn ""
    liftIO $ putStrLn ""
    liftIO $ putStrLn $ "Instrument: " ++ instrumentName
    liftIO $ putStrLn ""

    _ogroups <- getOrthancInstrumentGroups instrumentFiltersT <$> majorOrthancGroups

    -- Timezone:
    ZonedTime _ tz <- liftIO getZonedTime

    case _ogroups of Left err -> undefined
                     Right ogroups -> do
                                         let -- Times that available *series* have been updated:
                                             updatedTimes = map (\(_, _, s, _, _) -> (getSeriesLastUpdate tz s)) ogroups :: [Maybe ZonedTime]

                                             -- Hash of each:
                                             hashes = map (\(patient, study, series, _, _) -> getHashes (patient, study, series)) ogroups

                                             -- Together:
                                             hashAndLastUpdated = zip hashes updatedTimes

                                         liftIO $ putStrLn $ "|hashAndLastUpdated| = " ++ show (length hashAndLastUpdated)

                                         recentOgroups <- liftIO $ patientsToProcess acidMVar fp ogroups hashAndLastUpdated

                                         liftIO $ putStrLn $ "Experiments that are recent enough for us to process: " ++ show recentOgroups
                                         liftIO $ getZonedTime >>= print

                                         let fn = \og -> runReaderT (blaaah acidMVar experimentMVar datasetMVar groupMVar debug tz fp schemaExperiment schemaDataset schemaDicomFile defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators instrumentFilters instrumentMetadataFields experimentFields datasetFields og) conf

                                         return $ map fn recentOgroups

blaaah acidMVar experimentMVar datasetMVar groupMVar debug tz fp schemaExperiment schemaDataset schemaDicomFile defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators instrumentFilters instrumentMetadataFields experimentFields datasetFields (patient, study, series, oneInstance, tags) = do
    here <- liftIO getZonedTime
    liftIO $ print ("blaaah", "entering at time", here, opID patient, ostudyID study, oseriesID series)

    -- Before we get the archive, check if the Referring Physician is there so that we can get a project ID.
    let projectID = join $ tagValue <$> otagReferringPhysicianName tags
    case is5digits <$> projectID of
        Nothing    -> liftIO $ putStrLn $ "Error: could not read Referring Physician Name from " ++ opID patient ++ " " ++ ostudyID study ++ " " ++ oseriesID series
        Just False -> liftIO $ putStrLn $ "Error: invalid project ID: \"" ++ (fromJust projectID) ++ "\" from " ++ opID patient ++ " " ++ ostudyID study ++ " " ++ oseriesID series
        Just True  -> blaaah' acidMVar experimentMVar datasetMVar groupMVar debug tz fp schemaExperiment schemaDataset schemaDicomFile defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators instrumentFilters instrumentMetadataFields experimentFields datasetFields (patient, study, series, oneInstance, tags)

blaaah' acidMVar experimentMVar datasetMVar groupMVar debug tz fp schemaExperiment schemaDataset schemaDicomFile defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators instrumentFilters instrumentMetadataFields experimentFields datasetFields (patient, study, series, oneInstance, tags) = do

    liftIO $ getZonedTime >>= print
    liftIO $ putStrLn $ "getting series archive.... " ++ oseriesID series
    archive <- getSeriesArchive $ oseriesID series
    liftIO $ putStrLn $ "finished getting series archive.... " ++ oseriesID series

    case archive of
        Left e -> liftIO $ putStrLn $ "Error: could not get series archive " ++ oseriesID series ++ "; failed with error: " ++ e

        Right (tempDir, zipfile) -> do
            liftIO $ getZonedTime >>= print
            liftIO $ putStrLn $ "got series archive."

            liftIO $ print (tempDir, zipfile)

            tmp <- mytardisTmp <$> ask
            linksDir <- liftIO $ unpackArchive tmp tempDir zipfile
            case linksDir of
                Left linksErr -> liftIO $ putStrLn $ "blaaah: error unpacking archive: " ++ linksErr
                Right linksDir' -> do
                                      liftIO $ getZonedTime >>= print
                                      liftIO $ putStrLn $ "dostuff: linksDir: " ++ linksDir'

                                      createProjectGroup groupMVar linksDir'

                                      rawDicomFiles <- liftIO $ getDicomFilesInDirectory ".dcm" linksDir'
                                      anonymizationResults <- liftIO $ forM rawDicomFiles anonymizeDicomFile

                                      if length (lefts anonymizationResults) > 0
                                         then liftIO $ putStrLn $ "Errors while anonymizing DICOM files: " ++ show (lefts anonymizationResults)
                                         else do files <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" linksDir' >>= mapM readDicomMetadata)

                                                 liftIO $ getZonedTime >>= print
                                                 liftIO $ putStrLn $ "calling uploadDicomAsMincOneGroup..."
                                                 oneGroupResult <- uploadDicomAsMincOneGroup
                                                     experimentMVar
                                                     datasetMVar
                                                     files
                                                     instrumentFilters
                                                     instrumentMetadataFields
                                                     experimentFields
                                                     datasetFields
                                                     identifyExperiment
                                                     identifyDataset
                                                     identifyDatasetFile
                                                     linksDir'
                                                     ( schemaExperiment
                                                     , schemaDataset
                                                     , schemaDicomFile
                                                     , defaultInstitutionName
                                                     , defaultInstitutionalDepartmentName
                                                     , defaultInstitutionalAddress
                                                     , defaultOperators)

                                                 let schemaFile = schemaDicomFile -- FIXME

                                                 case oneGroupResult of
                                                     (A.Success (A.Success restExperiment, A.Success restDataset)) -> do
                                                             zipfile' <- uploadFileBasic schemaFile identifyDatasetFile restDataset zipfile [] -- FIXME add some metadata

                                                             case zipfile' of
                                                                 A.Success zipfile''   -> liftIO $ do putStrLn $ "Successfully uploaded: " ++ show zipfile''
                                                                                                      putStrLn $ "Deleting temporary directory: " ++ tempDir
                                                                                                      removeRecursiveSafely tempDir
                                                                                                      putStrLn $ "Deleting links directory: " ++ linksDir'
                                                                                                      removeRecursiveSafely linksDir'
                                                                                                      putStrLn $ "Updating last updated: " ++ show (fp, opID patient, getSeriesLastUpdate tz series)

                                                                                                      -- updateLastUpdate fp (getHashes (patient, study, series)) (getSeriesLastUpdate tz series)
                                                                                                      _ <- callWorkerIO acidMVar (AcidUpdateMap fp (getHashes (patient, study, series)) (getSeriesLastUpdate tz series))
                                                                                                      putStrLn $ "Updated last update."

                                                                 A.Error e             -> liftIO $ do putStrLn $ "Error while uploading series archive: " ++ e
                                                                                                      if debug then do putStrLn $ "Not deleting temporary directory: " ++ tempDir
                                                                                                                       putStrLn $ "Not deleting links directory: " ++ linksDir'
                                                                                                               else do putStrLn $ "Deleting temporary directory: " ++ tempDir
                                                                                                                       removeRecursiveSafely tempDir
                                                                                                                       putStrLn $ "Deleting links directory: " ++ linksDir'
                                                                                                                       removeRecursiveSafely linksDir'

                                                             liftIO $ print zipfile'
                                                     (A.Success (A.Error expError, _              )) -> liftIO $ putStrLn $ "Error when creating experiment: "     ++ expError
                                                     (A.Success (_,                A.Error dsError)) -> liftIO $ putStrLn $ "Error when creating dataset: "        ++ dsError
                                                     (A.Error e)                                     -> liftIO $ putStrLn $ "Error in uploadDicomAsMincOneGroup: " ++ e
    liftIO $ print "blaaah: exiting"

-}

dostuff :: UploaderOptions -> ReaderT MyTardisConfig IO ()

dostuff opts@(UploaderOptions _ _ _ _ (CmdShowExperiments cmdShow)) = do
    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \( instrumentName
                               , instrumentFilters
                               , instrumentFiltersT
                               , instrumentMetadataFields
                               , experimentFields
                               , datasetFields
                               , schemaExperiment
                               , schemaDataset
                               , schemaDicomFile
                               , defaultInstitutionName
                               , defaultInstitutionalDepartmentName
                               , defaultInstitutionalAddress
                               , defaultOperators)            -> do let groups = groupDicomFiles instrumentFilters experimentFields datasetFields _files
                                                                    forM_ groups $ \files -> do
                                                                        let
                                                                            Right (IdentifiedExperiment desc institution title metadata) = identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators experimentFields instrumentMetadataFields files -- FIXME dangerous pattern match
                                                                            hash = (sha256 . unwords) (map dicomFilePath files)

                                                                        liftIO $ if showFileSets cmdShow
                                                                            then printf "%s [%s] [%s] [%s] [%s]\n" hash institution desc title (unwords $ map dicomFilePath files)
                                                                            else printf "%s [%s] [%s] [%s]\n"      hash institution desc title

dostuff opts@(UploaderOptions _ _ _ _ (CmdUploadOne oneOpts)) = do
    let hash = uploadOneHash oneOpts

    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    let groups = concat $ flip map instrumentConfigs $ \( instrumentName
                               , instrumentFilters
                               , instrumentFiltersT
                               , instrumentMetadataFields
                               , experimentFields
                               , datasetFields
                               , schemaExperiment
                               , schemaDataset
                               , schemaDicomFile
                               , defaultInstitutionName
                               , defaultInstitutionalDepartmentName
                               , defaultInstitutionalAddress
                               , defaultOperators) -> groupDicomFiles instrumentFilters experimentFields datasetFields _files

    let
        hashes = map (hashFiles . fmap dicomFilePath) groups :: [String]
        matches = filter ((==) hash . snd) (zip groups hashes) :: [([DicomFile], String)]

    case matches of [match] -> liftIO $ print match
                    []      -> liftIO $ putStrLn "Hash does not match any identified experiment."
                    _       -> error "Multiple experiments with the same hash. This is a bug."

dostuff opts@(UploaderOptions _ _ _ _ (CmdUploadAll allOpts)) = do
    uploadAllAction opts

dostuff opts@(UploaderOptions _ _ _ _ (CmdUploadFromDicomServer dicomOpts)) = do
    if uploadFromDicomForever dicomOpts
        then do origDir <- liftIO getCurrentDirectory
                forever $ do liftIO $ setCurrentDirectory origDir
                             uploadDicomAction opts origDir
                             let sleepMinutes = uploadFromDicomSleepMinutes dicomOpts
                             liftIO $ printf "Sleeping for %d minutes...\n" sleepMinutes
                             liftIO $ threadDelay $ sleepMinutes * (60 * 10^6)
        else do origDir <- liftIO getCurrentDirectory
                uploadDicomAction opts origDir

    liftIO $ print "Exiting dostuff at top level."

dostuff opts@(UploaderOptions _ _ _ _ (CmdShowNewExperiments _)) = showNewAction opts

showNewAction opts = do
    -- FIXME This is all quite similar to the start of uploadDicomAction...

    -- Timezone:
    ZonedTime _ tz <- liftIO getZonedTime

    debug <- mytardisDebug <$> ask

    cwd <- liftIO getCurrentDirectory

    let slashToUnderscore = map (\c -> if c == '/' then '_' else c)

    let fp = cwd </> (slashToUnderscore $ "state_" ++ optConfigFile opts)
    liftIO $ createDirectoryIfMissing True fp

__imageTroveMain :: IO ()
__imageTroveMain = do
    opts' <- execParser opts

    let host = fromMaybe "http://localhost:8000" $ optHost opts'
        f    = optConfigFile opts'
        orthHost = "http://localhost:8043"
        debug    = optDebug opts'

    mytardisOpts <- getConfig host orthHost f debug

    case mytardisOpts of
        (Just mytardisOpts') -> runReaderT (dostuff opts') mytardisOpts'
        _                    -> error $ "Could not read config file: " ++ f

  where

    opts = info (helper <*> pUploaderOptions ) (fullDesc <> header "imagetrove-dicom-uploader - upload DICOM files to a MyTARDIS server" )

caiProjectID'' oneFile = _caiProjectID $ Just oneFile

caiProjectID' :: DFile -> A.Result String
caiProjectID' (DFile _ oneFile) = _caiProjectID $ Just oneFile

caiProjectID :: [DicomFile] -> A.Result String
caiProjectID files = let oneFile = headMay files in _caiProjectID oneFile

_caiProjectID oneFile = case oneFile of
        Nothing   -> A.Error "No DICOM files; can't determine CAI Project ID."
        Just file -> case dicomReferringPhysicianName file of
                        Nothing     -> A.Error "Referring Physician Name field is empty; can't determine CAI Project ID."
                        Just rphys  -> if is5digits rphys
                                            then A.Success rphys
                                            else A.Error   $ "Referring Physician Name is not a 5 digit number: " ++ rphys

is5digits :: String -> Bool
is5digits s = (length s == 5) && (isJust $ (readMaybe s :: Maybe Integer))

readMaybe :: (Read a) => String -> Maybe a
readMaybe s =
  case reads s of
      [(a, "")] -> Just a
      _         -> Nothing

identifyExperiment
    :: String
    -> String
    -> String
    -> String
    -> [String]
    -> [DicomFile -> Maybe String]
    -> [DicomFile -> Maybe String]
    -> [DicomFile]
    -> Either String IdentifiedExperiment
identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators titleFields instrumentFields files = do
    let title = join (allJust <$> (\f -> titleFields <*> [f]) <$> oneFile)
        _title = ((\f -> titleFields <*> [f]) <$> oneFile)

    case instrument of
        Nothing -> Left $ "Error: empty instrument when using supplied fields on file: " ++ show oneFile -- when (isNothing instrument) $ error $ "Error: empty instrument when using supplied fields on file: " ++ show oneFile
        Just instrument' -> do
                               let m' = M.insert "Instrument" instrument' m

                               let m'' = case caiProjectID files of
                                                   A.Success caiID -> M.insert "Project" ("Project " ++ caiID) m'
                                                   A.Error _       -> m'

                               case title of
                                   Nothing     -> Left $ "Error: empty experiment title when using supplied fields on file: " ++ show oneFile
                                   Just title' -> Right $ IdentifiedExperiment
                                                           description
                                                           institution
                                                           (intercalate "/" title')
                                                           [(schemaExperiment, m'')]
  where
    oneFile = headMay files

    -- patientName       = join $ dicomPatientName       <$> oneFile
    -- studyDescription  = join $ dicomStudyDescription  <$> oneFile
    -- seriesDescription = join $ dicomSeriesDescription <$> oneFile

    description = "" -- FIXME What should this be?

    -- institution = fromMaybe defaultInstitutionName $ join $ dicomInstitutionName <$> oneFile
    -- The InstitutionName field is missing for the Phoenix Zip Report and this results in two
    -- different experiments being created in MyTARDIS. So set the institution to be whatever's in the
    -- config file. Unlikely that a single config file will be run for multiple institutions?
    institution = defaultInstitutionName

    institutionalDepartmentName = defaultInstitutionalDepartmentName -- FIXME fromMaybe defaultInstitutionalDepartmentName $ join $ dicomInstitutionName    <$> oneFile
    -- institutionAddress          = fromMaybe defaultInstitutionalAddress        $ join $ dicomInstitutionAddress <$> oneFile
    institutionAddress          = defaultInstitutionalAddress

    instrument = (intercalate " ") <$> join (allJust <$> (\f -> instrumentFields <*> [f]) <$> oneFile)

    m = M.fromList $ m1 ++ m2

    m1 = [ ("InstitutionName",             institution)
         , ("InstitutionalDepartmentName", institutionalDepartmentName)
         , ("InstitutionAddress",          institutionAddress)
         ]

    -- m2 = map (\o -> ("Operator", o)) defaultOperators
    m2 = [("Operator", intercalate " " defaultOperators)]

allJust :: [Maybe a] -> Maybe [a]
allJust x = if all isJust x then Just (catMaybes x) else Nothing

identifyDataset :: String -> [DicomFile -> Maybe String] -> RestExperiment -> [DicomFile] -> Maybe IdentifiedDataset
identifyDataset schemaDataset datasetFields re files = let description = join (allJust <$> (\f -> datasetFields <*> [f]) <$> oneFile) in
    case description of
        Nothing           -> Nothing
        Just description' -> Just $ IdentifiedDataset
                                        (intercalate "/" description')
                                        experiments
                                        [(schemaDataset, m)]
  where
    oneFile = headMay files

    experiments = [eiResourceURI re]

    m           = M.fromList [ ("Manufacturer Model Name", fromMaybe "(ManufacturerModelName missing)" (join $ dicomManufacturerModelName <$> oneFile))
                             , ("Patient Name",            fromMaybe "(PatientName missing)"           (join $ dicomPatientName           <$> oneFile))
                             , ("Study Date",              fromMaybe "(Study Date missing)"            (join $ dicomStudyDate             <$> oneFile))
                             ]

identifyDatasetFile :: RestDataset -> String -> String -> Integer -> [(String, M.Map String String)] -> IdentifiedFile
identifyDatasetFile rds filepath md5sum size metadata = IdentifiedFile
                                        (dsiResourceURI rds)
                                        filepath
                                        md5sum
                                        size
                                        metadata

grabMetadata :: DicomFile -> [(String, String)]
grabMetadata file = map oops $ concatMap f metadata

  where

    oops (x, y) = (y, x)

    f :: (Maybe t, t) -> [(t, t)]
    f (Just x,  desc) = [(x, desc)]
    f (Nothing, _)    = []

    metadata =
        [ (dicomPatientName            file, "Patient Name")
        , (dicomPatientID              file, "Patient ID")
        , (dicomPatientBirthDate       file, "Patient Birth Date")
        , (dicomPatientSex             file, "Patient Sex")
        , (dicomPatientAge             file, "Patient Age")
        , (dicomPatientWeight          file, "Patient Weight")
        , (dicomPatientPosition        file, "Patient Position")

        , (dicomStudyDate              file, "Study Date")
        , (dicomStudyTime              file, "Study Time")
        , (dicomStudyDescription       file, "Study Description")
        -- , (dicomStudyInstanceID        file, "Study Instance ID")
        , (dicomStudyID                file, "Study ID")

        , (dicomSeriesDate             file, "Series Date")
        , (dicomSeriesTime             file, "Series Time")
        , (dicomSeriesDescription      file, "Series Description")
        -- , (dicomSeriesInstanceUID      file, "Series Instance UID")
        -- , (dicomSeriesNumber           file, "Series Number")
        -- , (dicomCSASeriesHeaderType    file, "CSA Series Header Type")
        -- , (dicomCSASeriesHeaderVersion file, "CSA Series Header Version")
        -- , (dicomCSASeriesHeaderInfo    file, "CSA Series Header Info")
        -- , (dicomSeriesWorkflowStatus   file, "Series Workflow Status")

        -- , (dicomMediaStorageSOPInstanceUID file, "Media Storage SOP Instance UID")
        -- , (dicomInstanceCreationDate       file, "Instance Creation Date")
        -- , (dicomInstanceCreationTime       file, "Instance Creation Time")
        -- , (dicomSOPInstanceUID             file, "SOP Instance UID")
        -- , (dicomStudyInstanceUID           file, "Study Instance UID")
        -- , (dicomInstanceNumber             file, "Instance Number")

        , (dicomInstitutionName                file, "Institution Name")
        , (dicomInstitutionAddress             file, "Institution Address")
        , (dicomInstitutionalDepartmentName    file, "Institutional Department Name")

        , (dicomReferringPhysicianName         file, "Referring Physician Name")
        ]



getConfig :: String -> String -> FilePath -> Bool -> IO (Maybe MyTardisConfig)
getConfig host orthHost f debug = do
    cfg <- load [Optional f]

    user    <- lookup cfg "user"    :: IO (Maybe String)
    pass    <- lookup cfg "pass"    :: IO (Maybe String)

    prefix  <- lookup cfg "prefix"  :: IO (Maybe String)

    ohost <- lookup cfg "orthanc_host" :: IO (Maybe String)
    let ohost' = if isNothing ohost then orthHost else fromJust ohost

    mytardisDir <- lookup cfg "mytardis_directory" :: IO (Maybe String)
    let mytardisDir' = if isNothing mytardisDir then "/imagetrove" else fromJust mytardisDir

    tmp <- lookup cfg "tmp" :: IO (Maybe String)
    let tmp' = if isNothing tmp then "/tmp" else fromJust tmp

    hSetBuffering stdin NoBuffering

    return $ case (user, pass, prefix) of
        (Just user', Just pass', Nothing)      -> Just $ defaultMyTardisOptions host user' pass' ohost' mytardisDir' debug tmp' ""
        (Just user', Just pass', Just prefix') -> Just $ defaultMyTardisOptions host user' pass' ohost' mytardisDir' debug tmp' prefix'
        _                                      -> Nothing

type InstrumentConfig = (String,
                         [DicomFile -> Bool],
                         [(String, String)],
                         [DicomFile -> Maybe String],
                         [DicomFile -> Maybe String],
                         [DicomFile -> Maybe String],
                         String, String, String, String, String, String, [String])

readInstrumentConfigs :: FilePath -> IO [InstrumentConfig]
readInstrumentConfigs f = do
    cfg <- load [Required f]

    instruments <- lookup cfg "instruments" :: IO (Maybe [String])

    case instruments of
        Nothing -> error $ "No instruments specified in configuration file: " ++ f
        Just instruments' -> mapM (readInstrumentConfig cfg . T.pack) instruments'

readInstrumentConfig :: Config -> T.Text -> IO InstrumentConfig
readInstrumentConfig cfg instrumentName = do
    instrumentFields <- liftM (map toIdentifierFn)    <$> lookup cfg (instrumentName `T.append` ".instrument")
    instrumentFieldsT<- liftM (map toIdentifierTuple) <$> lookup cfg (instrumentName `T.append` ".instrument")
    experimentFields <- liftM (map fieldToFn)         <$> lookup cfg (instrumentName `T.append` ".experiment_title")
    datasetFields    <- liftM (map fieldToFn)         <$> lookup cfg (instrumentName `T.append` ".dataset_title")

    -- TODO merge these together
    instrumentMetadataFields0 <- liftM (map $ \x -> fieldToFn <$> headMay x) <$> lookup cfg (instrumentName `T.append` ".instrument")
    let instrumentMetadataFields = join $ allJust <$> instrumentMetadataFields0

    schemaExperiment <- lookup cfg (instrumentName `T.append` ".schema_experiment")
    schemaDataset    <- lookup cfg (instrumentName `T.append` ".schema_dataset")
    schemaFile       <- lookup cfg (instrumentName `T.append` ".schema_file")

    defaultInstitutionName              <- lookup cfg (instrumentName `T.append` ".default_institution_name")
    defaultInstitutionalDepartmentName  <- lookup cfg (instrumentName `T.append` ".default_institutional_department_name")
    defaultInstitutionalAddress         <- lookup cfg (instrumentName `T.append` ".default_institutional_address")

    defaultOperators                    <- lookup cfg (instrumentName `T.append` ".default_operators")

    when (isNothing instrumentFields) $ error $ "Bad/missing 'instrument' field for "       ++ T.unpack instrumentName
    when (isNothing instrumentFieldsT)$ error $ "Bad/missing 'instrument' field for "       ++ T.unpack instrumentName
    when (isNothing experimentFields) $ error $ "Bad/missing 'experiment_title' field for " ++ T.unpack instrumentName
    when (isNothing datasetFields)    $ error $ "Bad/missing 'dataset_title' field for "    ++ T.unpack instrumentName

    when (isNothing schemaExperiment) $ error $ "Bad/missing 'schema_experiment' field for " ++ T.unpack instrumentName
    when (isNothing schemaDataset)    $ error $ "Bad/missing 'schema_dataset' field for "    ++ T.unpack instrumentName
    when (isNothing schemaFile)       $ error $ "Bad/missing 'schema_file' field for "       ++ T.unpack instrumentName

    when (isNothing defaultInstitutionName)              $ error $ "Bad/missing 'default_institution_name"              ++ T.unpack instrumentName
    when (isNothing defaultInstitutionalDepartmentName)  $ error $ "Bad/missing 'default_institutional_department_name" ++ T.unpack instrumentName
    when (isNothing defaultInstitutionalAddress)         $ error $ "Bad/missing 'default_institutional_address"         ++ T.unpack instrumentName

    when (isNothing defaultOperators)                    $ error $ "Bad/missing 'default_operators"                     ++ T.unpack instrumentName

    case ( instrumentFields
         , instrumentFieldsT
         , instrumentMetadataFields
         , experimentFields
         , datasetFields
         , schemaExperiment
         , schemaDataset
         , schemaFile
         , defaultInstitutionName
         , defaultInstitutionalDepartmentName
         , defaultInstitutionalAddress
         , defaultOperators
         ) of
        (Just instrumentFields', Just instrumentFieldsT', Just instrumentMetadataFields', Just experimentFields', Just datasetFields', Just schemaExperiment', Just schemaDataset', Just schemaFile', Just defaultInstitutionName', Just defaultInstitutionalDepartmentName', Just defaultInstitutionalAddress', Just defaultOperators') -> return (T.unpack instrumentName, instrumentFields', instrumentFieldsT', instrumentMetadataFields', experimentFields', datasetFields', schemaExperiment', schemaDataset', schemaFile', defaultInstitutionName', defaultInstitutionalDepartmentName', defaultInstitutionalAddress', defaultOperators')
        _ -> error "Error: unhandled case in readInstrumentConfig. Report this bug."

  where

    toIdentifierFn :: [String] -> DicomFile -> Bool
    toIdentifierFn [field, value] = tupleToIdentifierFn (field, value)
    toIdentifierFn x = error $ "Error: toIdentifierFn: too many items specified: " ++ show x

    toIdentifierTuple :: [String] -> (String, String)
    toIdentifierTuple [field, value] = (field, value)
    toIdentifierTuple x = error $ "Error: toIdentifierTuple: too many items specified: " ++ show x

type StudyDirectory = FilePath

data DFile = DFile {
    dfilePath      :: FilePath
  , dfileDicomInfo :: DicomFile
  } deriving Show

type DicomFilePath  = FilePath

groupBySeries :: [DicomFilePath] -> IO (Either String [[DicomFilePath]])
groupBySeries dfiles = do
    seriesUIDs <- withPool nrWorkersGlobal $ \pool -> parallel pool (Prelude.map readSeriesDesc dfiles)

    let uniqueUIDs = (S.toList . S.fromList) seriesUIDs
        result = Prelude.map (fmap snd) [ Prelude.filter (\(uid, _) -> uid == suid) (zip seriesUIDs dfiles) | suid <- uniqueUIDs ] -- inefficient...?
    putStrLn $ "groupBySeries: found " ++ show (length result) ++ " different series in this study."
    return $ Right result

copyToTempDir :: [DFile] -> ReaderT MyTardisConfig IO (A.Result FilePath)
copyToTempDir dfiles = copyToTempDir' dfiles `catchIOError` (\e -> return $ A.Error $ show (e :: IOException))
  where
    copyToTempDir' dfiles = do
        tmp     <- mytardisTmp <$> ask
        tempDir <- liftIO $ createTempDirectory tmp "dicom_temp"
        liftIO $ forM_ dfiles $ \(DFile d _) -> copyFile d tempDir
        return $ A.Success tempDir


data MVars = MVars (MVar (AcidAction,              MVar AcidOutput))
                   (MVar (IdentifiedExperiment,    MVar (Result RestExperiment)))
                   (MVar (IdentifiedDataset,       MVar (Result RestDataset)))
                   (MVar (String,                  MVar (Result RestGroup)))

imageTroveMain :: IO ()
imageTroveMain = do
    let host = "http://localhost:8020" -- "https://imagetrove.cai.uq.edu.au"
        f    = "debug_3T.conf"
        orthHost = "http://localhost:8043"
        debug    = False

    mytardisOpts <- getConfig host orthHost f debug

    case mytardisOpts of
        (Just mytardisOpts') -> do instrumentConfigs <- readInstrumentConfigs f -- FIXME use opts from cmdline parser (optConfigFile opts)
                                   -- forM_ instrumentConfigs $ \iconfig -> do let (_, _, instrumentFiltersT, _, _, _, _, _, _, _, _, _, _) = iconfig
                                   --                                          liftIO $ print instrumentFiltersT

                                   -- forM_ instrumentConfigs $ \iconfig -> runReaderT (runDCMTK f iconfig "/export/nif02/imagetrove/production/dcmtk-store-transfer/") mytardisOpts'
                                   -- runReaderT (runDCMTK f instrumentConfigs "/export/nif02/imagetrove/production/dcmtk-store-transfer/") mytardisOpts'
                                   runReaderT (runDCMTK f instrumentConfigs "/export/nif02/uqchamal/mytardis_develop/dcmtk_test_input/") mytardisOpts'
        _                    -> error $ "Could not read config file: " ++ f

nrWorkersGlobal = 20

type MyTardisIO = ReaderT MyTardisConfig IO

-- studyDirs :: FilePath -> DC.Source MyTardisIO (FilePath, String, UTCTime)
studyDirs :: FilePath -> IO [(FilePath, String, ZonedTime)]
studyDirs dir = getStableWithNameAndTime 5 dir

-- studyFiles :: DC.Conduit (FilePath, String, UTCTime) MyTardisIO (FilePath, String, UTCTime, [(DicomFilePath, String)])
-- studyFiles = CL.mapM $ \(sdir, dirname, mtime) -> liftIO $ do
studyFiles :: (FilePath, String, ZonedTime) -> IO (FilePath, String, ZonedTime, [(FilePath, String)])
studyFiles (sdir, dirname, mtime) = do
    files <- getDicomFilesInDirectory ".IMA" sdir
    suids <- withPool nrWorkersGlobal $ \pool -> parallel pool (map readSeriesDesc files)

    putStrLn $ "studyFiles: found " ++ show (length files) ++ " files in " ++ sdir

    return $ (sdir, dirname, mtime, zip files suids)

type StudyDir = FilePath
type StudyDirName = String
type StudyLastUpdate = ZonedTime

type SeriesDesc = String

type Series = [(FilePath, SeriesDesc)]

type DCMTKSeries = (StudyDir, StudyDirName, StudyLastUpdate, Series)

_groupBySeries :: DCMTKSeries -> [DCMTKSeries]
_groupBySeries (studyDir, studyDirName, studyLastUpdate, series) = map (\s -> (studyDir, studyDirName, studyLastUpdate, s)) series'
  where
    f ds = groupBy (\(_, s1) (_, s2) -> s1 == s2) ((sortBy . comparing) snd ds)
    series' = f series

sink :: DC.Sink [(DicomFilePath, String)] IO ()
sink = CL.mapM_ print

getBasicMetadata :: DicomFilePath -> IO (String, String, String, String)
getBasicMetadata d = do
    m <- readDicomMetadata d
    case m of
        Left e -> throwM $ OtherImagetroveException $ "Could not read DICOM metadata from " ++ d ++ ": " ++ e
        Right m' -> case (dicomPatientID m', dicomStudyDescription m', dicomSeriesNumber m', dicomSeriesDescription m') of
                      (Just patientID, Just studyDesc, Just seriesNr, Just seriesDesc) -> return (patientID, studyDesc, seriesNr, seriesDesc)
                      x -> throwM $ MetadataMissingException x

getSeriesDir :: [(DicomFilePath, String)] -> IO String
getSeriesDir [] = throwM $ EmptySeriesException "Got a series with no files in getSeriesDir."
getSeriesDir ((d, _):_) = do
    (patientID, studyDesc, seriesNr, seriesDesc) <- getBasicMetadata d
    return $ tidyName $ patientID </> studyDesc </> (seriesDesc ++ seriesNr)

tidyName = makeValid . map (toLower . fixWeirdChars)

-- What others? I bet there are more.
fixWeirdChars '-' = '_'
fixWeirdChars '.' = '_'
fixWeirdChars ':' = '_'
fixWeirdChars c   = c

genZipFileName :: [(DicomFilePath, String)] -> IO String
genZipFileName [] = throwM $ EmptySeriesException "Got a series with no files in genZipFileName."
genZipFileName ((d, _):_) = do
    (patientID, studyDesc, seriesNr, seriesDesc) <- getBasicMetadata d
    -- return $ (tidyName $ patientID ++ "_" ++ studyDesc ++ "_" ++ (seriesDesc ++ seriesNr)) ++ ".zip"
    -- return $ (tidyName $ patientID ++ "_" ++ studyDesc ++ "_" ++ seriesDesc) ++ ".zip"
    return $ patientID ++ "_" ++ studyDesc ++ "_" ++ seriesDesc ++ "_DICOM.zip"

processSeries :: String -> InstrumentConfig -> MVars -> DCMTKSeries -> MyTardisIO ()
processSeries acidDir iconfig mvars dcmtkSeries = do
    let (studyDir, studyDirName, studyLastUpdate, series) = dcmtkSeries

    let dfiles = series

    liftIO $ putStrLn $ "processSeries: got series of " ++ show (length dfiles) ++ " files."

    let MVars acidMVar experimentMVar datasetMVar groupMVar = mvars
    -- h <- liftIO $ getStudyDirName $ map fst dfiles
    -- liftIO $ putStrLn $ "Series has " ++ (show $ length dfiles) ++ " files with hashes: " ++ show h

    let f = fst <$> headMay dfiles
    m <- liftIO $ traverse readDicomMetadata f

    case m of
        Nothing       -> liftIO $ putStrLn "Error: no files in series."
        Just (Left e) -> do liftIO $ putStrLn $ "Error: Could not read DICOM metadata: " ++ e
                            liftIO $ putStrLn $ "Error: could not read DICOM metadata: " ++ e ++ " from file: " ++ show f

        Just (Right m') -> do let projectID = caiProjectID [m']
                              liftIO $ putStrLn $ "Found project ID: " ++ show projectID

                              projectGroup <- _createProjectGroup groupMVar projectID
                              case projectGroup of
                                A.Error e -> liftIO $ putStrLn $ "Could not create project group " ++ show projectID ++ " due to error: " ++ e
                                A.Success _ -> do liftIO $ putStrLn $ "Created project group: " ++ show projectID

                                                  -- Create resources (temp directories, etc) for the actual
                                                  -- processing of the series. This is not done in a tidy way,
                                                  -- dirs won't be removed if an exception occurs.
                                                  tmp <- mytardisTmp <$> ask
                                                  tempDir <- liftIO $ createTempDirectory tmp "dcmtk_conversion"

                                                  seriesDirName <- liftIO $ (tempDir </>) <$> (getSeriesDir dfiles)
                                                  liftIO $ createDirectoryIfMissing True seriesDirName

                                                  let resources = Resources tempDir seriesDirName

                                                  (finishSeries acidDir mvars iconfig resources dcmtkSeries) `finally` (liftIO $ removeRecursiveSafely tempDir)

                                                  liftIO $ print ()

copyDicoms :: [FilePath] -> FilePath -> IO [FilePath]
copyDicoms dfiles targetDir = do
    forM_ (zip dfiles targetFiles) (\(old, new) -> do when (old == new) (error $ "Trying to copy file to itself: " ++ old)
                                                      putStrLn $ old ++ " ==> " ++ new
                                                      copyFile old new)
    return targetFiles
  where
    targetFiles  = map (\f -> targetDir </> (takeFileName f)) dfiles

makeTemp :: String -> MyTardisIO (Either String FilePath)
makeTemp desc = do
    tmp <- mytardisTmp <$> ask

    catch (liftIO $ Right <$> (createTempDirectory tmp desc))
          (\e -> return $ Left $ show (e :: IOException))

finishSeries :: String -> MVars -> InstrumentConfig -> Resources -> DCMTKSeries -> ReaderT MyTardisConfig IO ()
finishSeries acidDir mvars iconfig resources dcmtkSeries = do
    let (studyDir, studyDirName, studyLastUpdate, series) = dcmtkSeries
        dfiles    = map fst series
        _dfiles   = series

        tempDir   = resourceTemp resources
        seriesDir = resourceSeriesDir resources

    let MVars acidMVar experimentMVar datasetMVar groupMVar = mvars

    let (instrumentName,
         instrumentFilters,
         instrumentFiltersT,
         instrumentMetadataFields,
         experimentFields,
         datasetFields,
         schemaExperiment,
         schemaDataset,
         schemaFile,
         defaultInstitutionName,
         defaultInstitutionalDepartmentName,
         defaultInstitutionalAddress,
         defaultOperators) = iconfig

    -- OK, we have created the project group, so now we need to do the various stages...
    --
    -- stage 1: make copies
    copiedDicomFiles <- liftIO $ copyDicoms dfiles seriesDir

    _dicomMetadata <- liftIO $ forM copiedDicomFiles readDicomMetadata
    when (lefts _dicomMetadata /= []) $ throwM $ OtherImagetroveException $ "Could not read DICOM metadata: " ++ show (lefts _dicomMetadata)
    let dicomMetadata = rights _dicomMetadata

    -- stage 2: anonymize
    liftIO $ withPool nrWorkersGlobal $ \pool -> parallel pool (map anonymizeDicomFile' copiedDicomFiles)
    anonymizedDicomFiles <- liftIO $ getDicomFilesInDirectory ".IMA" seriesDir

    -- stage 3: make zip of dicom
    zipFileName <- liftIO $ genZipFileName _dfiles
    _patientDir  <- liftIO $ (map $ last . splitPath) <$> (getDirs' tempDir)

    when (length _patientDir /= 1) (error $ "Found multiple patient directories in " ++ tempDir ++ " ==> " ++ show _patientDir)
    let [patientDir] = _patientDir

    liftIO $ putStrLn $ "Running: " ++ show (tempDir, "zip", ["-r", zipFileName, patientDir])
    _ <- liftIO $ _runShellCommand tempDir "zip" ["-r", zipFileName, patientDir]

    let zipFileFullName = tempDir </> zipFileName

    -- stage 4: make minc
    _mincFiles <- liftIO $ dicomToMinc tempDir dfiles -- IO (Either String (FilePath, [FilePath])) -- FIXME stupid pattern match

    mincFiles <- case _mincFiles of
                    Left e -> throwM $ OtherImagetroveException e
                    Right _mincFiles' -> return _mincFiles'

    -- stage 4a: rename minc files
    renamedMincFiles <- liftIO $ renameMinc dfiles mincFiles

    -- stage 4a(i): MINC to MINC2
    tmp <- mytardisTmp <$> ask
    toMinc2Results <- liftIO $ forM renamedMincFiles (mncToMnc2 tmp) -- FIXME check results

    -- stage 4b: minc thumbnails
    mincThumbnails <- liftIO $ forM renamedMincFiles createMincThumbnail

    -- stage 5: make nifti
    niftis <- liftIO $ forM renamedMincFiles createNifti

    let files = renamedMincFiles

    -- stage 6: make experiment/dataset
    schemas <- createSchemasIfMissing (schemaExperiment, schemaDataset, schemaFile)

    let ie = identifyExperiment schemaExperiment
                                defaultInstitutionName
                                defaultInstitutionalDepartmentName
                                defaultInstitutionalAddress
                                defaultOperators
                                experimentFields
                                instrumentMetadataFields
                                dicomMetadata

    e <- eitherToResult <$> traverse (callWorker experimentMVar) ie :: ReaderT MyTardisConfig IO (Result RestExperiment)

    let ids = resultMaybeToResult ((\e -> identifyDataset schemaDataset datasetFields e dicomMetadata) <$> e) :: Result IdentifiedDataset

    d <- squashResults <$> traverse (callWorker datasetMVar) ids :: ReaderT MyTardisConfig IO (Result RestDataset)

    let oneFile = headMay dicomMetadata
        Just pid        = join (dicomPatientID         <$> oneFile) -- FIXME dangerous pattern match, should be an error
        Just studyDesc  = join (dicomStudyDescription  <$> oneFile) -- FIXME dangerous pattern match, should be an error
        Just seriesDesc = join (dicomSeriesDescription <$> oneFile) -- FIXME dangerous pattern match, should be an error
        seriesNr        = fromMaybe "(series)" $ join (dicomSeriesNumber <$> oneFile)
        filemetadata = (\f -> [(schemaFile, M.fromList
                                                [ ("PatientID",         fromMaybe "(PatientID missing)"         $ dicomPatientID         f)
                                                -- , ("StudyInstanceUID",  fromMaybe "(StudyInstanceUID missing)"  $ dicomStudyInstanceUID  f)
                                                -- , ("SeriesInstanceUID", fromMaybe "(SeriesInstanceUID missing)" $ dicomSeriesInstanceUID f)
                                                , ("StudyDescription",  fromMaybe "(StudyDescription missing)"  $ dicomStudyDescription  f)
                                                ]
                               )
                              ]) <$> oneFile

    -- stage 7: push files
    case (e, d, filemetadata) of
        (A.Success e', A.Success d', Just filemetadata') -> do let push = pushFile schemaFile identifyDatasetFile d' filemetadata'
                                                               -- Zip of DICOM files:
                                                               push zipFileFullName

                                                               -- MINC files:
                                                               forM_ renamedMincFiles push

                                                               -- MINC thumbnails:
                                                               forM_ (rights mincThumbnails) push

                                                               -- Nifti files:
                                                               forM_ (rights niftis) push

                                                               -- FIXME produce warnings about lefts of mincThumbnails and niftis

                                                               liftIO $ do callWorkerIO acidMVar (AcidUpdateMap acidDir studyDirName studyLastUpdate)
                                                                           putStrLn $ "Updated last update."

                                                               liftIO $ print "done"

        (A.Error expError, _, _)            -> throwM $ OtherImagetroveException $ "Error when creating experiment: " ++ expError
        (A.Success _, A.Error dsError, _)   -> throwM $ OtherImagetroveException $ "Error when creating dataset: " ++ dsError
        (A.Success _, A.Success _, Nothing) -> throwM $ OtherImagetroveException $ "Error when creating extracting file metadata. No files in DICOM group!"

    liftIO $ print "finishSeries: done."

pushFile schemaFile identifyDatasetFile d filemetadata f = do
    dsf <- uploadFileBasic schemaFile identifyDatasetFile d f filemetadata

    case dsf of
        A.Error e   -> throwM $ OtherImagetroveException $ "Could not upload file: " ++ e
        A.Success _ -> return ()

renameMinc dfiles (tempDir, _mincFiles) = do
    m@(pid, studyDesc, seriesNr, seriesDesc) <- getBasicMetadata (head dfiles)
    putStrLn $ "renameMinc: metadata: " ++ show m

    -- let renamer pid studyDesc seriesNr seriesDesc f = base </> (pid ++ "_" ++ studyDesc ++ "_" ++ seriesDesc ++ "_" ++ seriesNr ++ "_" ++ f')
    let renamer pid studyDesc seriesNr seriesDesc f = base </> (pid ++ "_" ++ studyDesc ++ "_" ++ seriesDesc ++ f')
           where base = dropFileName f
                 (originalMincFile:dirName:_) = reverse $ splitPath f
                 f' = drop (length dirName - 1) originalMincFile

    let myRenamer = renamer pid studyDesc seriesNr seriesDesc

    liftIO $ forM_ _mincFiles $ \f -> do let old = f
                                             new = myRenamer old
                                         putStrLn $ "Renaming <" ++ old ++ "> to <" ++ new ++ ">"
                                         rename old new
    return $ map myRenamer _mincFiles

isStudyNewer m (_, studyDirName, studyLastUpdate) = case M.lookup studyDirName m of
                                                            Nothing         -> True -- Haven't seen this before so we must process it.
                                                            Just luPrevious -> zonedTimeToUTC luPrevious < zonedTimeToUTC studyLastUpdate -- Check if current update time is newer.

chooseConfig :: [InstrumentConfig] -> (FilePath, String, ZonedTime) -> IO InstrumentConfig
chooseConfig iconfigs s@(studyDir, studyDirName, studyLastUpdate) = do
    files <- getDicomFilesInDirectory ".IMA" studyDir

    case files of
        []       -> throwM $ OtherImagetroveException $ "Empty study directory: " ++ studyDir
        (file:_) -> do m <- readDicomMetadata file
                       case m of
                         Left err -> throwM $ OtherImagetroveException $ "Could not read DICOM metadata from file " ++ file
                         Right m' -> case filter (isMatch m') iconfigs of
                                        []        -> throwM $ OtherImagetroveException $ "No matching instrument for study in directory " ++ studyDir
                                        [iconfig] -> return iconfig
                                        ics       -> throwM $ OtherImagetroveException $ "Multiple instrument matches for study in directory " ++ studyDir

  where
    isMatch :: DicomFile -> InstrumentConfig -> Bool
    isMatch m iconfig = let (_, _, instrumentFiltersT, _, _, _, _, _, _, _, _, _, _) = iconfig in
                            allTrue (map mkSelector instrumentFiltersT) m

    allTrue fns x = and [ f x | f <- fns ]
 
    mkSelector :: (String, String) -> DicomFile -> Bool
    mkSelector (fieldName, expectedValue) = \dicomFile -> (fieldToFn fieldName) dicomFile == Just expectedValue


processStudy :: [InstrumentConfig] -> MVars -> FilePath -> (FilePath, String, ZonedTime) -> MyTardisIO [Maybe SomeException]
processStudy iconfigs mvars fp s@(studyDir, studyDirName, studyLastUpdate) = do
    let MVars acidMVar _ _ _ = mvars

    AcidMap m <- callWorker acidMVar (AcidLoadMap fp)

    iconfig <- liftIO $ chooseConfig iconfigs s

    if (isStudyNewer m s)
        then do liftIO $ print "done"
                sf <- liftIO $ studyFiles s
                -- sf <- liftIO $ withPool 5 $ \pool -> parallel pool (map studyFiles s)

                -- Break apart into series
                let series = _groupBySeries sf

                conf <- ask
                let seriesTasks = map (\s -> runReaderT (processSeries fp iconfig mvars s) conf) series

                results <- liftIO $ withPool nrWorkersGlobal $ \pool -> parallelE_ pool seriesTasks
                liftIO $ forM_ results print -- FIXME tidy up
                return results -- ... or print this stuff one level up
        else return []

runDCMTK configFileName iconfigs topDir = do
    liftIO $ hSetBuffering stdout LineBuffering

    conf <- ask

    cwd <- liftIO getCurrentDirectory

    let slashToUnderscore = map (\c -> if c == '/' then '_' else c)
        fp = cwd </> (slashToUnderscore $ "state_" ++ configFileName)

    liftIO $ createDirectoryIfMissing True fp

    acidMVar       <- liftIO $ newEmptyMVar
    experimentMVar <- liftIO $ newEmptyMVar
    datasetMVar    <- liftIO $ newEmptyMVar
    groupMVar      <- liftIO $ newEmptyMVar

    let mvars = MVars acidMVar experimentMVar datasetMVar groupMVar

    asyncAcidWorker             <- liftIO $ async $ acidWorker acidMVar
    asyncWorkerCreateExperiment <- liftIO $ async $ runReaderT (workerCreateExperiment experimentMVar) conf
    asyncWorkerCreateDataset    <- liftIO $ async $ runReaderT (workerCreateDataset    datasetMVar)    conf
    asyncWorkerCreateGroup      <- liftIO $ async $ runReaderT (workerCreateGroup      groupMVar)      conf

    liftIO $ putStrLn $ "runDCMTK..."

    AcidMap currentMap <- callWorker acidMVar (AcidLoadMap fp)

    s  <- liftIO $ studyDirs topDir
    liftIO $ putStrLn $ "runDCMTK found " ++ (show $ length s) ++ " study directories."

    let k = 3

    forM_ (splitEvery k s) $ \chunk -> do let tasks = map (\c -> runReaderT (processStudy iconfigs mvars fp c) conf) chunk
                                          liftIO $ putStrLn $ "Spawning " ++ show k ++ " jobs..."
                                          liftIO $ withPool k $ \pool -> parallel pool tasks

    pollExperiment <- liftIO $ poll asyncWorkerCreateExperiment
    pollDataset    <- liftIO $ poll asyncWorkerCreateDataset
    pollGroup      <- liftIO $ poll asyncWorkerCreateGroup
    pollAcid       <- liftIO $ poll asyncAcidWorker

    liftIO $ putStrLn $ "poll pollExperiment: " ++ show pollExperiment
    liftIO $ putStrLn $ "poll pollDataset: " ++ show pollDataset
    liftIO $ putStrLn $ "poll pollGroup: " ++ show (pollGroup :: Maybe (Either SomeException ()))
    liftIO $ putStrLn $ "poll pollAcid: " ++ show (pollAcid :: Maybe (Either SomeException ()))

    liftIO $ print "runDCMTK: exiting"

data Resources = Resources {
    resourceTemp        :: FilePath     -- For processing a single series, a top level temp directory.
  , resourceSeriesDir   :: FilePath     -- Nicely named directory for DICOM series files; lives in 'resourceCopyTemp'
} deriving (Show)

