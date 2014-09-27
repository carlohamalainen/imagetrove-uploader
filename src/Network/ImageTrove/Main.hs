{-# LANGUAGE OverloadedStrings #-}

module Network.ImageTrove.Main where

import Prelude hiding (lookup)

import Control.Monad.Reader
import Control.Monad (when)
import Data.Configurator
import Data.Configurator.Types
import Data.Monoid (mempty)
import Data.Traversable (traverse)
import Data.Either
import qualified Data.Foldable as DF
import Data.Maybe
import Data.List (intercalate)
import qualified Data.Map as M
import qualified Data.Aeson as A
import qualified Data.Text as T
import Options.Applicative
import Safe (headMay)
import Text.Printf (printf)

import Data.Dicom
import Network.ImageTrove.Utils
import Network.MyTardis.API
import Network.MyTardis.RestTypes
import Network.MyTardis.Types
import Network.Orthanc.API

data Command
    = CmdUploadAll       UploadAllOptions
    | CmdUploadOne       UploadOneOptions
    | CmdShowExperiments ShowExperimentsOptions
    | CmdUploadFromDicomServer UploadFromDicomServerOptions
    deriving (Eq, Show)

data UploadAllOptions = UploadAllOptions { uploadAllDryRun :: Bool } deriving (Eq, Show)
data UploadOneOptions = UploadOneOptions { uploadOneHash :: String } deriving (Eq, Show)

data ShowExperimentsOptions = ShowExperimentsOptions { showFileSets :: Bool } deriving (Eq, Show)

data UploadFromDicomServerOptions = UploadFromDicomServerOptions { uploadFromDicomDryRun :: Bool } deriving (Eq, Show)

data UploaderOptions = UploaderOptions
    { optDirectory      :: Maybe FilePath
    , optHost           :: Maybe String
    , optConfigFile     :: FilePath
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
pUploadFromDicomServerOptions = CmdUploadFromDicomServer <$> UploadFromDicomServerOptions <$> switch (long "dry-run" <> help "Dry run.")

pUploaderOptions :: Parser UploaderOptions
pUploaderOptions = UploaderOptions
    <$> optional (strOption (long "input-dir"     <> metavar "DIRECTORY" <> help "Directory with DICOM files."))
    <*> optional (strOption (long "host"          <> metavar "HOST"      <> help "MyTARDIS host URL, e.g. http://localhost:8000"))
    <*>          (strOption (long "config"        <> metavar "CONFIG"    <> help "Configuration file."))
    <*> subparser x
  where
    x    = cmd1 <> cmd2 <> cmd3 <> cmd4
    cmd1 = command "upload-all"               (info (helper <*> pUploadAllOptions) (progDesc "Upload all experiments."))
    cmd2 = command "upload-one"               (info (helper <*> pUploadOneOptions) (progDesc "Upload a single experiment."))
    cmd3 = command "show-experiments"         (info (helper <*> pShowExprOptions)  (progDesc "Show local experiments."))
    cmd4 = command "upload-from-dicom-server" (info (helper <*> pUploadFromDicomServerOptions) (progDesc "Upload from DICOM server."))

getDicomDir :: UploaderOptions -> FilePath
getDicomDir opts = fromMaybe "." (optDirectory opts)

hashFiles :: [FilePath] -> String
hashFiles = sha256 . unwords

dostuff :: UploaderOptions -> ReaderT MyTardisConfig IO ()

dostuff opts@(UploaderOptions _ _ _ (CmdShowExperiments cmdShow)) = do
    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \( instrumentFilters
                               , instrumentFiltersT
                               , experimentFields
                               , datasetFields
                               , schemaExperiment
                               , schemaDataset
                               , schemaDicomFile
                               , defaultInstitutionName
                               , defaultInstitutionalDepartmentName
                               , defaultInstitutionalAddress) -> do let groups = groupDicomFiles instrumentFilters experimentFields datasetFields _files
                                                                    forM_ groups $ \files -> do
                                                                        let
                                                                            Just (IdentifiedExperiment desc institution title metadata) = identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress experimentFields files
                                                                            hash = (sha256 . unwords) (map dicomFilePath files)

                                                                        liftIO $ if showFileSets cmdShow
                                                                            then printf "%s [%s] [%s] [%s] [%s]\n" hash institution desc title (unwords $ map dicomFilePath files)
                                                                            else printf "%s [%s] [%s] [%s]\n"      hash institution desc title


dostuff opts@(UploaderOptions _ _ _ (CmdUploadAll allOpts)) = do
    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \(instrumentFilters, instrumentFiltersT, experimentFields, datasetFields, schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress) -> uploadDicomAsMinc instrumentFilters experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile (getDicomDir opts) (schemaExperiment, schemaDataset, schemaDicomFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress)

    -- FIXME Just for testing, create some accounts and assign all experiments to these users.
    A.Success project12345 <- getOrCreateGroup "Project 12345"

    cHamalainen <- getOrCreateUser (Just "Carlo")  (Just "Hamalainen") "c.hamalainen@uq.edu.au" [project12345] True

    A.Success experiments <- getExperiments
    forM_ experiments $ (flip addGroupReadOnlyAccess) project12345

dostuff opts@(UploaderOptions _ _ _ (CmdUploadOne oneOpts)) = do
    let hash = uploadOneHash oneOpts

    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    let groups = concat $ (flip map) instrumentConfigs $ \( instrumentFilters
                               , instrumentFiltersT
                               , experimentFields
                               , datasetFields
                               , schemaExperiment
                               , schemaDataset
                               , schemaDicomFile
                               , defaultInstitutionName
                               , defaultInstitutionalDepartmentName
                               , defaultInstitutionalAddress) -> groupDicomFiles instrumentFilters experimentFields datasetFields _files

    let
        hashes = map (hashFiles . fmap dicomFilePath) groups :: [String]
        matches = filter ((==) hash . snd) (zip groups hashes) :: [([DicomFile], String)]

    case matches of [match] -> liftIO $ print match
                    []      -> liftIO $ putStrLn "Hash does not match any identified experiment."
                    _       -> error "Multiple experiments with the same hash. Oh noes!"

dostuff opts@(UploaderOptions _ _ _ (CmdUploadFromDicomServer _)) = do
    instrumentConfigs <- liftIO $ readInstrumentConfigs (optConfigFile opts)

    forM_ instrumentConfigs $ \( instrumentFilters
                               , instrumentFiltersT
                               , experimentFields
                               , datasetFields
                               , schemaExperiment
                               , schemaDataset
                               , schemaDicomFile
                               , defaultInstitutionName
                               , defaultInstitutionalDepartmentName
                               , defaultInstitutionalAddress) -> do
            Right ogroups <- liftIO $ getOrthancInstrumentGroups instrumentFiltersT <$> majorOrthancGroups

            forM_ ogroups $ \(patient, study, series, oneInstance, tags) -> do
                Right (tempDir, zipfile) <- liftIO $ getSeriesArchive $ oseriesID series

                liftIO $ print (tempDir, zipfile)

                -- FIXME Tidy up links and temp dir.

                Right linksDir <- liftIO $ unpackArchive tempDir zipfile
                liftIO $ putStrLn $ "dostuff: linksDir: " ++ linksDir

                files <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" linksDir >>= mapM readDicomMetadata)

                (restExperiment, restDataset) <- uploadDicomAsMincOneGroup
                    files
                    instrumentFilters
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
                    , defaultInstitutionalAddress)

                let schemaFile = schemaDicomFile -- FIXME

                zipfile' <- uploadFileBasic schemaFile identifyDatasetFile restDataset zipfile [] -- FIXME add some metadata
                liftIO $ print zipfile'

    -- FIXME Just for testing, create some accounts and assign all experiments to these users.
    A.Success project12345 <- getOrCreateGroup "Project 12345"

    cHamalainen <- getOrCreateUser (Just "Carlo")  (Just "Hamalainen") "c.hamalainen@uq.edu.au" [project12345] True

    A.Success experiments <- getExperiments
    forM_ experiments $ (flip addGroupReadOnlyAccess) project12345

imageTroveMain :: IO ()
imageTroveMain = do
    opts' <- execParser opts

    let host = fromMaybe "http://localhost:8000" $ optHost opts'
        f    = optConfigFile opts'

    mytardisOpts <- getConfig host f

    case mytardisOpts of
        (Just mytardisOpts') -> runReaderT (dostuff opts') mytardisOpts'
        _                    -> error $ "Could not read config file: " ++ f

  where

    opts = info (helper <*> pUploaderOptions ) (fullDesc <> header "imagetrove-uploader - upload DICOM files to a MyTARDIS server" )

{-
caiProjectID :: [DicomFile] -> A.Result [PS]
caiProjectID files = let oneFile = headMay files in
    case oneFile of
        Nothing   -> A.Error "No DICOM files; can't determine CAI Project ID."
        Just file -> case dicomReferringPhysicianName file of
                        Nothing     -> A.Error "Referring Physician Name field is empty; can't determine CAI Project ID."
                        Just rphys  -> if is5digits rphys
                                            then A.Success [mkCaiExperimentPS rphys]
                                            else A.Error $ "Referring Physician Name is not a 5 digit number: " ++ rphys

  where

    is5digits :: String -> Bool
    is5digits s = (length s == 5) && (isJust $ (readMaybe s :: Maybe Integer))

    readMaybe :: (Read a) => String -> Maybe a
    readMaybe s =
      case reads s of
          [(a, "")] -> Just a
          _         -> Nothing
-}

identifyExperiment
    :: String
    -> String
    -> String
    -> String
    -> [DicomFile -> Maybe String]
    -> [DicomFile]
    -> Maybe IdentifiedExperiment
identifyExperiment schemaExperiment defaultInstitutionName defaultInstitutionalDepartmentName defaultInstitutionalAddress titleFields files =
    let title = join (allJust <$> (\f -> titleFields <*> [f]) <$> oneFile) in
        case title of
            Nothing -> Nothing
            Just title' -> Just $ IdentifiedExperiment
                                    description
                                    institution
                                    (intercalate "/" title')
                                    [(schemaExperiment, m)]
  where
    oneFile = headMay files

    patientName       = join $ dicomPatientName       <$> oneFile
    studyDescription  = join $ dicomStudyDescription  <$> oneFile
    seriesDescription = join $ dicomSeriesDescription <$> oneFile

    description = "" -- FIXME What should this be?

    institution = fromMaybe defaultInstitutionName $ join $ dicomInstitutionName <$> oneFile

    institutionalDepartmentName = defaultInstitutionalDepartmentName -- FIXME fromMaybe defaultInstitutionalDepartmentName $ join $ dicomInstitutionName    <$> oneFile
    institutionAddress          = fromMaybe defaultInstitutionalAddress        $ join $ dicomInstitutionAddress <$> oneFile

    m = M.fromList
            [ ("InstitutionName",             institution)
            , ("InstitutionalDepartmentName", institutionalDepartmentName)
            , ("InstitutionAddress",          institutionAddress)
            ]

allJust :: [Maybe a] -> Maybe [a]
allJust x = if and (map isJust x) then Just (catMaybes x) else Nothing

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

    m           = M.fromList [ ("ManufacturerModelName", fromMaybe "(ManufacturerModelName missing)" (join $ dicomManufacturerModelName <$> oneFile)) ]

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



getConfig :: String -> FilePath -> IO (Maybe MyTardisConfig)
getConfig host f = do
    cfg <- load [(Optional f)]

    user <- lookup cfg "user" :: IO (Maybe String)
    pass <- lookup cfg "pass" :: IO (Maybe String)

    return $ case (user, pass) of
        (Just user', Just pass') -> Just $ defaultMyTardisOptions host user' pass'
        _                        -> Nothing


readInstrumentConfigs
  :: FilePath
     -> IO
          [([DicomFile -> Bool],
            [(String, String)],
            [DicomFile -> Maybe String],
            [DicomFile -> Maybe String],
            String, String, String, String, String, String)]
readInstrumentConfigs f = do
    cfg <- load [(Required f)]

    instruments <- lookup cfg "instruments" :: IO (Maybe [String])

    case instruments of
        Nothing -> error $ "No instruments specified in configuration file " ++ f
        Just instruments' -> mapM (readInstrumentConfig cfg) (map T.pack instruments')

readInstrumentConfig
  :: Config
       -> T.Text
       -> IO
            ([DicomFile -> Bool],
             [(String, String)],
             [DicomFile -> Maybe String],
             [DicomFile -> Maybe String],
             String, String, String, String, String, String)
readInstrumentConfig cfg instrumentName = do
    instrumentFields <- (liftM $ map toIdentifierFn) <$> lookup cfg (instrumentName `T.append` ".instrument")
    instrumentFieldsT<- (liftM $ map toIdentifierTuple) <$> lookup cfg (instrumentName `T.append` ".instrument")
    experimentFields <- (liftM $ map fieldToFn)      <$> lookup cfg (instrumentName `T.append` ".experiment_title")
    datasetFields    <- (liftM $ map fieldToFn)      <$> lookup cfg (instrumentName `T.append` ".dataset_title")

    schemaExperiment <- lookup cfg (instrumentName `T.append` ".schema_experiment")
    schemaDataset    <- lookup cfg (instrumentName `T.append` ".schema_dataset")
    schemaFile       <- lookup cfg (instrumentName `T.append` ".schema_file")

    defaultInstitutionName              <- lookup cfg (instrumentName `T.append` ".default_institution_name")
    defaultInstitutionalDepartmentName  <- lookup cfg (instrumentName `T.append` ".default_institutional_department_name")
    defaultInstitutionalAddress         <- lookup cfg (instrumentName `T.append` ".default_institutional_address")

    when (isNothing instrumentFields) $ error $ "Bad/missing 'instrument' field for "       ++ (T.unpack instrumentName)
    when (isNothing instrumentFieldsT)$ error $ "Bad/missing 'instrument' field for "       ++ (T.unpack instrumentName)
    when (isNothing experimentFields) $ error $ "Bad/missing 'experiment_title' field for " ++ (T.unpack instrumentName)
    when (isNothing datasetFields)    $ error $ "Bad/missing 'dataset_title' field for "    ++ (T.unpack instrumentName)

    when (isNothing schemaExperiment) $ error $ "Bad/missing 'schema_experiment' field for " ++ (T.unpack instrumentName)
    when (isNothing schemaDataset)    $ error $ "Bad/missing 'schema_dataset' field for "    ++ (T.unpack instrumentName)
    when (isNothing schemaFile)       $ error $ "Bad/missing 'schema_file' field for "       ++ (T.unpack instrumentName)

    when (isNothing defaultInstitutionName)              $ error $ "Bad/missing 'default_institution_name"              ++ (T.unpack instrumentName)
    when (isNothing defaultInstitutionalDepartmentName)  $ error $ "Bad/missing 'default_institutional_department_name" ++ (T.unpack instrumentName)
    when (isNothing defaultInstitutionalAddress)         $ error $ "Bad/missing 'default_institutional_address"         ++ (T.unpack instrumentName)

    case ( instrumentFields
         , instrumentFieldsT
         , experimentFields
         , datasetFields
         , schemaExperiment
         , schemaDataset
         , schemaFile
         , defaultInstitutionName
         , defaultInstitutionalDepartmentName
         , defaultInstitutionalAddress
         ) of
        (Just instrumentFields', Just instrumentFieldsT', Just experimentFields', Just datasetFields', Just schemaExperiment', Just schemaDataset', Just schemaFile', Just defaultInstitutionName', Just defaultInstitutionalDepartmentName', Just defaultInstitutionalAddress') -> return (instrumentFields', instrumentFieldsT', experimentFields', datasetFields', schemaExperiment', schemaDataset', schemaFile', defaultInstitutionName', defaultInstitutionalDepartmentName', defaultInstitutionalAddress')
        _ -> error "Unhandled case."

  where

    toIdentifierFn :: [String] -> (DicomFile -> Bool)
    toIdentifierFn [field, value] = tupleToIdentifierFn (field, value)
    toIdentifierFn x = error $ "Too many items specified: " ++ show x

    toIdentifierTuple :: [String] -> (String, String)
    toIdentifierTuple [field, value] = (field, value)
    toIdentifierTuple x = error $ "Too many items specified: " ++ show x


