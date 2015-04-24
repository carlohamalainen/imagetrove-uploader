{-# LANGUAGE OverloadedStrings #-}

module Network.ImageTrove.MainDicom where

import Prelude hiding (lookup)

import Control.Monad.Reader
import Control.Monad (forever, when)
import Control.Concurrent (threadDelay)
import Data.Configurator
import Data.Configurator.Types
import Data.Monoid (mempty)
import Data.Traversable (traverse)
import Data.Either
import qualified Data.Foldable as DF
import Data.Maybe
import Data.List (isPrefixOf, intercalate)
import qualified Data.Map as M
import qualified Data.Aeson as A
import qualified Data.Text as T
import Options.Applicative
import Safe (headMay)
import Text.Printf (printf)

import qualified Data.Set as S

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

import System.Directory (getCurrentDirectory, setCurrentDirectory, createDirectoryIfMissing)
import System.FilePath ((</>))

import Network.ImageTrove.Acid

import qualified Data.Map as Map

data Command
    = CmdUploadAll       UploadAllOptions
    | CmdUploadOne       UploadOneOptions
    | CmdShowExperiments ShowExperimentsOptions
    | CmdUploadFromDicomServer UploadFromDicomServerOptions
    deriving (Eq, Show)

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
    x    = cmd4
    cmd1 = command "upload-all"               (info (helper <*> pUploadAllOptions) (progDesc "Upload all experiments."))
    cmd2 = command "upload-one"               (info (helper <*> pUploadOneOptions) (progDesc "Upload a single experiment."))
    cmd3 = command "show-experiments"         (info (helper <*> pShowExprOptions)  (progDesc "Show local experiments."))
    cmd4 = command "upload-from-dicom-server" (info (helper <*> pUploadFromDicomServerOptions) (progDesc "Upload from DICOM server."))

getDicomDir :: UploaderOptions -> FilePath
getDicomDir opts = fromMaybe "." (optDirectory opts)

hashFiles :: [FilePath] -> String
hashFiles = sha256 . unwords

createProjectGroup linksDir = do
    projectID <- liftIO $ caiProjectID <$> rights <$> (getDicomFilesInDirectory ".dcm" linksDir >>= mapM readDicomMetadata)

    case projectID of A.Success projectID' -> do projectResult <- getOrCreateGroup $ "Project " ++ projectID'
                                                 case projectResult of A.Success _              -> liftIO $ putStrLn $ "Created project group: " ++ projectID'
                                                                       A.Error   projErr        -> liftIO $ putStrLn $ "Error when creating project group: " ++ projErr
                      A.Error   err        -> liftIO $ putStrLn $ "Error: could not retrieve Project ID from ReferringPhysician field: " ++ err

uploadAllAction opts = do
    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \(instrumentFilters, instrumentFiltersT, instrumentMetadataFields, experimentFields, datasetFields, schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress, defaultOperators) -> uploadDicomAsMinc instrumentFilters instrumentMetadataFields experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile (getDicomDir opts) (schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress, defaultOperators)

-- | Orthanc returns the date/time but has no timezone so append tz here:
-- getPatientLastUpdate :: TimeZone -> OrthancPatient -> Maybe ZonedTime
-- getPatientLastUpdate tz p = utcToZonedTime tz <$> parseTime defaultTimeLocale "%Y%m%dT%H%M%S %Z" (opLastUpdate p ++ " " ++ show tz)

-- | Orthanc returns the date/time but has no timezone so append tz here:
getSeriesLastUpdate :: TimeZone -> OrthancSeries -> Maybe ZonedTime
getSeriesLastUpdate tz s = utcToZonedTime tz <$> parseTime defaultTimeLocale "%Y%m%dT%H%M%S %Z" (oseriesLastUpdate s ++ " " ++ show tz)

getHashes :: (OrthancPatient, OrthancStudy, OrthancSeries) -> PatientStudySeries
getHashes (patient, study, series) = (opID patient, ostudyID study, oseriesID series)

patientsToProcess :: FilePath
                  -> [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
                  -> [(PatientStudySeries, Maybe ZonedTime)]
                  -> IO [(OrthancPatient, OrthancStudy, OrthancSeries, OrthancInstance, OrthancTags)]
patientsToProcess fp ogroups hashAndLastUpdated = do
    m <- loadMap fp -- :: FilePath -> IO (M.Map String (Maybe ZonedTime))

    let blah = zip ogroups hashAndLastUpdated

    return $ map fst $ filter (f m) blah

  where
    f m (_, (h, Just lu)) = case M.lookup h m of
                                Nothing                 -> True -- hash is not in the map
                                Just Nothing            -> True -- hash is in the map but no ZonedTime available (perhaps due to parse error from Orthanc's LastUpdate field?)
                                Just (Just luPrevious)  -> zonedTimeToUTC luPrevious < zonedTimeToUTC lu  -- hash and LastUpdate in map, compare to see if current update time is newer.
    f m (_, (h, Nothing)) = True


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

                                      runShellCommand cmd opts

uploadDicomAction opts origDir = do
    -- Timezone:
    ZonedTime _ tz <- liftIO getZonedTime

    debug <- mytardisDebug <$> ask

    cwd <- liftIO getCurrentDirectory

    let slashToUnderscore = map (\c -> if c == '/' then '_' else c)

    let fp = cwd </> (slashToUnderscore $ "state_" ++ optConfigFile opts)
    liftIO $ createDirectoryIfMissing True fp

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \( instrumentFilters
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
                               , defaultOperators) -> do
            _ogroups <- getOrthancInstrumentGroups instrumentFiltersT <$> majorOrthancGroups

            case _ogroups of Left err -> undefined
                             Right ogroups -> do
                                                 let -- Times that available *series* have been updated:
                                                     updatedTimes = map (\(_, _, s, _, _) -> (getSeriesLastUpdate tz s)) ogroups :: [Maybe ZonedTime]

                                                     -- Hash of each:
                                                     hashes = map (\(patient, study, series, _, _) -> getHashes (patient, study, series)) ogroups

                                                     -- Together:
                                                     hashAndLastUpdated = zip hashes updatedTimes

                                                 liftIO $ putStrLn $ "|hashAndLastUpdated| = " ++ show (length hashAndLastUpdated)

                                                 recentOgroups <- liftIO $ patientsToProcess fp ogroups hashAndLastUpdated

                                                 liftIO $ putStrLn $ "Experiments that are recent enough for us to process: " ++ show recentOgroups
                                                 liftIO $ getZonedTime >>= print

                                                 forM_ recentOgroups $ \(patient, study, series, oneInstance, tags) -> do
                                                     liftIO $ getZonedTime >>= print
                                                     liftIO $ putStrLn $ "getting series archive...."
                                                     Right (tempDir, zipfile) <- getSeriesArchive $ oseriesID series
                                                     liftIO $ getZonedTime >>= print
                                                     liftIO $ putStrLn $ "got series archive."

                                                     liftIO $ print (tempDir, zipfile)

                                                     Right linksDir <- liftIO $ unpackArchive tempDir zipfile
                                                     liftIO $ getZonedTime >>= print
                                                     liftIO $ putStrLn $ "dostuff: linksDir: " ++ linksDir

                                                     createProjectGroup linksDir

                                                     rawDicomFiles <- liftIO $ getDicomFilesInDirectory ".dcm" linksDir
                                                     anonymizationResults <- liftIO $ forM rawDicomFiles anonymizeDicomFile

                                                     if length (lefts anonymizationResults) > 0
                                                        then liftIO $ putStrLn $ "Errors while anonymizing DICOM files: " ++ show (lefts anonymizationResults)
                                                        else do files <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" linksDir >>= mapM readDicomMetadata)

                                                                liftIO $ getZonedTime >>= print
                                                                liftIO $ putStrLn $ "calling uploadDicomAsMincOneGroup..."
                                                                oneGroupResult <- uploadDicomAsMincOneGroup
                                                                    files
                                                                    instrumentFilters
                                                                    instrumentMetadataFields
                                                                    experimentFields
                                                                    datasetFields
                                                                    identifyExperiment
                                                                    identifyDataset
                                                                    identifyDatasetFile
                                                                    linksDir
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
                                                                                                                     putStrLn $ "Deleting links directory: " ++ linksDir
                                                                                                                     removeRecursiveSafely linksDir
                                                                                                                     putStrLn $ "Updating last updated: " ++ show (fp, opID patient, getSeriesLastUpdate tz series)
                                                                                                                     updateLastUpdate fp (getHashes (patient, study, series)) (getSeriesLastUpdate tz series)
                                                                                A.Error e             -> liftIO $ do putStrLn $ "Error while uploading series archive: " ++ e
                                                                                                                     if debug then do putStrLn $ "Not deleting temporary directory: " ++ tempDir
                                                                                                                                      putStrLn $ "Not deleting links directory: " ++ linksDir
                                                                                                                              else do putStrLn $ "Deleting temporary directory: " ++ tempDir
                                                                                                                                      removeRecursiveSafely tempDir
                                                                                                                                      putStrLn $ "Deleting links directory: " ++ linksDir
                                                                                                                                      removeRecursiveSafely linksDir

                                                                            liftIO $ print zipfile'
                                                                    (A.Success (A.Error expError, _              )) -> liftIO $ putStrLn $ "Error when creating experiment: "     ++ expError
                                                                    (A.Success (_,                A.Error dsError)) -> liftIO $ putStrLn $ "Error when creating dataset: "        ++ dsError
                                                                    (A.Error e)                                     -> liftIO $ putStrLn $ "Error in uploadDicomAsMincOneGroup: " ++ e

dostuff :: UploaderOptions -> ReaderT MyTardisConfig IO ()

dostuff opts@(UploaderOptions _ _ _ _ (CmdShowExperiments cmdShow)) = do
    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \( instrumentFilters
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
                                                                            Just (IdentifiedExperiment desc institution title metadata) = identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators experimentFields instrumentMetadataFields files
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

    let groups = concat $ flip map instrumentConfigs $ \( instrumentFilters
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

imageTroveMain :: IO ()
imageTroveMain = do
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

caiProjectID :: [DicomFile] -> A.Result String
caiProjectID files = let oneFile = headMay files in
    case oneFile of
        Nothing   -> A.Error "No DICOM files; can't determine CAI Project ID."
        Just file -> case dicomReferringPhysicianName file of
                        Nothing     -> A.Error "Referring Physician Name field is empty; can't determine CAI Project ID."
                        Just rphys  -> if is5digits rphys
                                            then A.Success rphys
                                            else A.Error   $ "Referring Physician Name is not a 5 digit number: " ++ rphys

  where

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
    -> Maybe IdentifiedExperiment
identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress defaultOperators titleFields instrumentFields files = do
    let title = join (allJust <$> (\f -> titleFields <*> [f]) <$> oneFile)
        _title = ((\f -> titleFields <*> [f]) <$> oneFile)

    when (isNothing instrument) $ error $ "Error: empty instrument when using supplied fields on file: " ++ show oneFile

    let m' = M.insert "Instrument" (fromJust instrument) m

    let m'' = case caiProjectID files of
                        A.Success caiID -> M.insert "Project" ("Project " ++ caiID) m'
                        A.Error _       -> m'

    case title of
        Nothing -> error $ "Error: empty experiment title when using supplied fields on file: " ++ show oneFile
        Just title' -> Just $ IdentifiedExperiment
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

    ohost <- lookup cfg "orthanc_host" :: IO (Maybe String)
    let ohost' = if isNothing ohost then orthHost else fromJust ohost

    mytardisDir <- lookup cfg "mytardis_directory" :: IO (Maybe String)
    let mytardisDir' = if isNothing mytardisDir then "/imagetrove" else fromJust mytardisDir

    hSetBuffering stdin NoBuffering

    return $ case (user, pass) of
        (Just user', Just pass') -> Just $ defaultMyTardisOptions host user' pass' ohost' mytardisDir' debug
        _                        -> Nothing

readInstrumentConfigs
  :: FilePath
     -> IO
          [([DicomFile -> Bool],
            [(String, String)],
            [DicomFile -> Maybe String],
            [DicomFile -> Maybe String],
            [DicomFile -> Maybe String],
            String, String, String, String, String, String, [String])]
readInstrumentConfigs f = do
    cfg <- load [Required f]

    instruments <- lookup cfg "instruments" :: IO (Maybe [String])

    case instruments of
        Nothing -> error $ "No instruments specified in configuration file: " ++ f
        Just instruments' -> mapM (readInstrumentConfig cfg . T.pack) instruments'

readInstrumentConfig
  :: Config
       -> T.Text
       -> IO
            ([DicomFile -> Bool],
             [(String, String)],
             [DicomFile -> Maybe String],
             [DicomFile -> Maybe String],
             [DicomFile -> Maybe String],
             String, String, String, String, String, String, [String])
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
        (Just instrumentFields', Just instrumentFieldsT', Just instrumentMetadataFields', Just experimentFields', Just datasetFields', Just schemaExperiment', Just schemaDataset', Just schemaFile', Just defaultInstitutionName', Just defaultInstitutionalDepartmentName', Just defaultInstitutionalAddress', Just defaultOperators') -> return (instrumentFields', instrumentFieldsT', instrumentMetadataFields', experimentFields', datasetFields', schemaExperiment', schemaDataset', schemaFile', defaultInstitutionName', defaultInstitutionalDepartmentName', defaultInstitutionalAddress', defaultOperators')
        _ -> error "Error: unhandled case in readInstrumentConfig. Report this bug."

  where

    toIdentifierFn :: [String] -> DicomFile -> Bool
    toIdentifierFn [field, value] = tupleToIdentifierFn (field, value)
    toIdentifierFn x = error $ "Error: toIdentifierFn: too many items specified: " ++ show x

    toIdentifierTuple :: [String] -> (String, String)
    toIdentifierTuple [field, value] = (field, value)
    toIdentifierTuple x = error $ "Error: toIdentifierTuple: too many items specified: " ++ show x
