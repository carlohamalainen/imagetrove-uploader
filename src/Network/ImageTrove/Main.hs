module Network.ImageTrove.Main where

import Control.Monad.Reader
import Control.Monad (when)
import Data.Configurator
import Data.Traversable (traverse)
import Data.Either
import qualified Data.Foldable as DF
import Data.Maybe
import qualified Data.Map as M
import qualified Data.Aeson as A
import Options.Applicative
import Safe (headMay)
import Text.Printf (printf)

import Data.Dicom
import Network.ImageTrove.Utils
import Network.MyTardis.API
import Network.MyTardis.MyTardis hiding (defaultMyTardisOptions, getDatasets, getExperiment, getParameterNames, MyTardisConfig(..))
import Network.MyTardis.RestTypes
import Network.MyTardis.Types

data Command
    = CmdUploadAll       UploadAllOptions
    | CmdUploadOne       UploadOneOptions
    | CmdShowExperiments ShowExperimentsOptions
    deriving (Eq, Show)

data UploadAllOptions = UploadAllOptions { uploadAllDryRun :: Bool } deriving (Eq, Show)
data UploadOneOptions = UploadOneOptions { uploadOneHash :: String } deriving (Eq, Show)

data ShowExperimentsOptions = ShowExperimentsOptions { showFileSets :: Bool } deriving (Eq, Show)

data UploaderOptions = UploaderOptions
    { optDirectory  :: Maybe FilePath
    , optHost       :: Maybe String
    , optUser       :: Maybe String
    , optProcessedDir :: FilePath
    , optCommand    :: Command
    }
    deriving (Eq, Show)

pUploadAllOptions :: Parser Command
pUploadAllOptions = CmdUploadAll <$> UploadAllOptions <$> switch (long "dry-run" <> help "Dry run.")

pUploadOneOptions :: Parser Command
pUploadOneOptions = CmdUploadOne <$> UploadOneOptions <$> strOption (long "hash" <> help "Hash of experiment to upload.")

pShowExprOptions :: Parser Command
pShowExprOptions = CmdShowExperiments <$> ShowExperimentsOptions <$> switch (long "show-file-sets" <> help "Show experiments.")

pUploaderOptions :: Parser UploaderOptions
pUploaderOptions = UploaderOptions
    <$> optional (strOption (long "input-dir"     <> metavar "DIRECTORY" <> help "Directory with DICOM files."))
    <*> optional (strOption (long "host"          <> metavar "HOST"      <> help "MyTARDIS host URL, e.g. http://localhost:8000"))
    <*> optional (strOption (long "user"          <> metavar "USERNAME"  <> help "MyTARDIS username."))
    <*>          (strOption (long "processed-dir" <> metavar "DIRECTORY" <> help "Directory for processed DICOM files."))
    <*> subparser x
  where
    x    = cmd1 <> cmd2 <> cmd3
    cmd1 = command "upload-all"       (info (helper <*> pUploadAllOptions) (progDesc "Upload all experiments."))
    cmd2 = command "upload-one"       (info (helper <*> pUploadOneOptions) (progDesc "Upload a single experiment."))
    cmd3 = command "show-experiments" (info (helper <*> pShowExprOptions)  (progDesc "Show local experiments."))

getDicomDir :: UploaderOptions -> FilePath
getDicomDir opts = fromMaybe "." (optDirectory opts)

hashFiles :: [FilePath] -> String
hashFiles = sha256 . unwords

dostuff :: UploaderOptions -> ReaderT MyTardisConfig IO ()

dostuff opts@(UploaderOptions _ _ _ _ (CmdShowExperiments cmdShow)) = do
    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    let groups = groupDicomFiles _files

    liftIO $ print groups

    forM_ groups $ \files -> do
        let
            IdentifiedExperiment desc institution title metadata = identifyExperiment files
            hash = (sha256 . unwords) (map dicomFilePath files)

        liftIO $ if showFileSets cmdShow
            then printf "%s [%s] [%s] [%s] [%s]\n" hash institution desc title (unwords $ map dicomFilePath files)
            else printf "%s [%s] [%s] [%s]\n"      hash institution desc title

dostuff opts@(UploaderOptions _ _ _ _ (CmdUploadAll allOpts)) = uploadDicomAsMinc identifyExperiment identifyDataset identifyDatasetFile (getDicomDir opts) (optProcessedDir opts) (schemaExperiment, schemaDataset, schemaDicomFile)

dostuff opts@(UploaderOptions _ _ _ _ (CmdUploadOne oneOpts)) = do
    let hash = uploadOneHash oneOpts

    let dir = getDicomDir opts

    -- FIXME let user specify glob
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2

    let groups = groupDicomFiles _files

    let
        hashes = map (hashFiles . fmap dicomFilePath) groups :: [String]
        matches = filter ((==) hash . snd) (zip groups hashes) :: [([DicomFile], String)]

    case matches of [match] -> liftIO $ print match
                    []      -> liftIO $ putStrLn "Hash does not match any identified experiment."
                    _       -> error "Multiple experiments with the same hash. Oh noes!"

{-
main = do
    config <- load [Required "sample.conf"]

    display config
-}

dicomMain :: IO ()
dicomMain = do
    opts' <- execParser opts

    let
        host = fromMaybe "http://localhost:8000" $ optUser opts'
        user = fromMaybe "admin"                 $ optHost opts'
        pass = "admin" -- FIXME Fail on no password

        mytardisOpts = (defaultMyTardisOptions host user pass)

    runReaderT (dostuff opts') mytardisOpts

  where

    opts = info (helper <*> pUploaderOptions ) (fullDesc <> header "mytardis-dicom - upload DICOM files to a MyTARDIS server" )

testtmp = flip runReaderT (defaultMyTardisOptions "http://localhost:8000" "admin" "admin") blah
  where
    blah :: ReaderT MyTardisConfig IO ()
    blah = do
        {-
        let dir = "/tmp/dicomdump"

        _files <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
        let files = head $ groupDicomFiles _files

        x <- getExperimentWithMetadata (identifyExperiment files)

        liftIO $ print x
        -}

        {-
        forM_ [1..50] $ \i -> do
            e <- createExperiment $ IdentifiedExperiment (show i) "UQ" (show i) []
            liftIO $ print e
        -}

        {-
            A.Success users <- getUsers

            let admin = head $ filter (\x -> ruserUsername x == "admin") users


            A.Success cai12345 <- getOrCreateGroup "CAI 12345"

            liftIO $ print admin
            liftIO $ print cai12345

            A.Success experiment <- getExperiment "/api/v1/experiment/3/"
            liftIO $ print experiment

            -- addGroupReadOnlyAccess experiment cai12345


            -- liftIO $ print "Up to here..."

            -- newacl <- createExperimentObjectACL cai12345 experiment False True False

            -- liftIO $ print newacl

            eacl <- getOrCreateExperimentACL experiment cai12345
            liftIO $ print ("eacl", eacl)
     
            -- experiment' <- addGroupReadOnlyAccess experiment cai12345
            -- liftIO $ print ("experiment'", experiment')

            -- user' <- addGroupToUser admin cai12345
            -- liftIO $ print user'
        -}

        {-
        A.Success cai12346 <- getOrCreateGroup "CAI 12346"
        u <- createUser (Just "firstname") (Just "lastname") "user@uq.edu.au" [cai12346] False
        liftIO $ print u
        -}

        A.Success pnames <- getParameterNames

        let pmap = M.fromList $ map (\pn -> (pnResourceURI pn, pnName pn)) pnames

        liftIO $ print pmap

        A.Success datasets <- getDatasets

        forM_ datasets $ \d -> do
            case (hasField pmap "ManufacturerModelName" d) of
                Just (_, value) -> do experiments <- mapM getExperiment (dsiExperiments d)
                                      forM_ experiments $ \e -> do
                                            -- FIXME at this point add operator access....
                                            liftIO $ print e
                _               -> liftIO $ print "no manufacturer field name..."


        -- liftIO $ print $ map (hasField pmap "ManufacturerModelName") datasets

        --liftIO $ print datasets


        liftIO $ print "done"
   

hasField :: M.Map String String -> t -> RestDataset -> Maybe (String, String)
hasField pnmap fieldName dataset = case derp' of
    [(name, value)]     -> Just (name, value)
    _                   -> Nothing
  where
    pset = concatMap dsParamSetParameters $ dsiParameterSets dataset

    derp = map (\x -> (M.lookup (epName x) pnmap, epStringValue x)) pset

    derp' = concatMap fn derp

    fn (Just x, Just y) = [(x, y)]
    fn _                = []

newtype PS = PS (String, M.Map String String)
    deriving (Eq, Show)

data Flabert = Flabert
    { idExperiment  :: [DicomFile] -> IdentifiedExperiment                  -- ^ Identify an experiment.
    , idDataset     :: RestExperiment -> [DicomFile] -> IdentifiedDataset   -- ^ Given a particular experiment on MyTARDIS, identify the local dataset.


     -- ^ Given a particular dataset in MyTARDIS, identify ,
     , idDatasetFile :: RestDataset                         -- ^ Dataset on MyTARDIS.
                     -> FilePath                            -- ^ Full path to file.
                     -> String                              -- ^ Md5sum.
                     -> Integer                             -- ^ File size.
                     -> [(String, M.Map String String)]     -- ^ Metadata map.
                     -> IdentifiedFile
    }
    
    -- [ [DicomFile] -> A.Result [PS] ]

-- noFlaberts = Flabert []

mkCaiExperimentPS :: String -> PS
mkCaiExperimentPS pid = PS ("http://cai.uq.edu.au/schema/1", undefined) -- FIXME hardcoded

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



runThingos :: [Flabert] -> [DicomFile] -> A.Result [PS]
runThingos flab files = undefined
  where
    stuff = undefined -- flab <*> files



-- FIXME Testing, need to work out what these will be later.
experimentTitlePrefix = "CAI Test Experiment "
experimentDescriptionPrefix = "CAI Test Experiment Description"
datasetDescription = "CAI Dataset Description"

-- FIXME These should be in a reader or something.
schemaExperiment  = "http://cai.uq.edu.au/schema/metadata/1"
schemaDataset     = "http://cai.uq.edu.au/schema/metadata/2"
schemaDicomFile   = "http://cai.uq.edu.au/schema/metadata/3"
schemaCaiProject  = "http://cai.uq.edu.au/schema/metadata/4"

defaultInstitutionName = "DEFAULT INSTITUTION"

identifyExperiment :: [DicomFile] -> IdentifiedExperiment
identifyExperiment files = IdentifiedExperiment
                                description
                                institution
                                title
                                [(schema, m)]
  where
    oneFile = headMay files

    patientName       = join $ dicomPatientName       <$> oneFile
    studyDescription  = join $ dicomStudyDescription  <$> oneFile
    seriesDescription = join $ dicomSeriesDescription <$> oneFile

    -- Experiment
    title       = fromMaybe "DUD TITLE FIXME" $ (experimentTitlePrefix ++) <$> patientName
    description = experimentDescriptionPrefix

    institution = fromMaybe "DEFAULT INSTITUTION FIXME" $ join $ dicomInstitutionName <$> oneFile

    institutionalDepartmentName = fromMaybe "DEFAULT INSTITUTION DEPT NAME FIXME" $ join $ dicomInstitutionName    <$> oneFile
    institutionAddress          = fromMaybe "DEFAULT INSTITUTION ADDRESS FIXME" $ join $ dicomInstitutionAddress <$> oneFile

    patientID = fromMaybe "FIXME DUD PATIENT ID" $ join $ dicomPatientID <$> oneFile -- FIXME dangerous? Always get a patient id?

    -- FIXME deal with multiple schemas.
    schema = "http://cai.uq.edu.au/schema/metadata/1" -- "DICOM Experiment Metadata"

    m = M.fromList
            [ ("InstitutionName",             institution)
            , ("InstitutionalDepartmentName", institutionalDepartmentName)
            , ("InstitutionAddress",          institutionAddress)
            , ("PatientID",                   patientID)
            ]

identifyDataset :: RestExperiment -> [DicomFile] -> IdentifiedDataset
identifyDataset re files = IdentifiedDataset
                                description
                                experiments
                                [(schemaDataset, m)]
  where
    oneFile = head files -- FIXME this will explode, use headMay instead
    description = (fromMaybe "FIXME STUDY DESCRIPTION" $ dicomStudyDescription oneFile) ++ "/" ++ (fromMaybe "FIXME SERIES DESCRIPTION" $ dicomSeriesDescription oneFile)
    experiments = [eiResourceURI re]
    schema      = schemaDataset

    m           = M.fromList [ ("ManufacturerModelName", fromMaybe "(ManufacturerModelName missing)" (dicomManufacturerModelName oneFile)) ]

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

