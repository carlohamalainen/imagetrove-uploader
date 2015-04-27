{-# LANGUAGE OverloadedStrings #-}

module Network.ImageTrove.MainBruker where

import Prelude hiding (lookup)

import Control.Exception.Base (catch, IOException(..))

import Control.Monad.Reader
import Control.Monad (forever, when)
import Control.Concurrent (threadDelay)
import Data.Char (isDigit)
import Data.Configurator
import Data.Configurator.Types
import Data.Monoid (mempty)
import Data.Traversable (traverse)
import Data.Either
import qualified Data.Foldable as DF
import Data.Maybe
import Data.List (isPrefixOf, isSuffixOf, intercalate)
import qualified Data.Map as M
import qualified Data.Aeson as A
import qualified Data.Text as T
import Options.Applicative
import Safe (headMay)
import Text.Printf (printf)

import System.IO.Temp

import Data.Dicom
import Network.ImageTrove.Utils
import Network.MyTardis.API
import Network.MyTardis.RestTypes
import Network.MyTardis.Types
import Network.Orthanc.API

import System.Posix.Files
import System.IO

import Control.Concurrent (threadDelay)

import System.Unix.Directory (removeRecursiveSafely)

import Data.Time.Clock (addUTCTime, getCurrentTime, diffUTCTime, UTCTime(..), NominalDiffTime(..))
import Data.Time.LocalTime (getZonedTime, utcToZonedTime, zonedTimeToUTC, TimeZone(..), ZonedTime(..))
import Data.Time.Format (parseTime)
import Data.Time.Clock.POSIX
import System.Locale (defaultTimeLocale)

import System.Directory
import System.FilePath

import Network.ImageTrove.Acid

import Network.ImageTrove.Bruker

import qualified Data.Map as Map

import System.Unix.Directory (removeRecursiveSafely)

data Command = CmdUploadAll UploadAllOptions
    deriving (Eq, Show)

data UploadAllOptions = UploadAllOptions deriving (Eq, Show)

data UploaderOptions = UploaderOptions
    { optHost           :: Maybe String
    , optConfigFile     :: FilePath
    , optDebug          :: Bool
    , optCommand        :: Command
    }
    deriving (Eq, Show)

pUploadAllOptions :: Parser Command
pUploadAllOptions = CmdUploadAll <$> (pure UploadAllOptions)

pUploaderOptions :: Parser UploaderOptions
pUploaderOptions = UploaderOptions
    <$> optional (strOption (long "host"          <> metavar "HOST"      <> help "MyTARDIS host URL, e.g. http://localhost:8000"))
    <*>          (strOption (long "config"        <> metavar "CONFIG"    <> help "Configuration file."))
    <*>          (switch    (long "debug"                                <> help "Debug mode."))
    <*> subparser x
  where
    x    = cmd1
    cmd1 = command "upload-all"               (info (helper <*> pUploadAllOptions) (progDesc "Upload all experiments."))

getConfig :: String -> FilePath -> Bool -> IO (Maybe MyTardisConfig)
getConfig host f debug = do
    cfg <- load [Optional f]

    user    <- lookup cfg "user"    :: IO (Maybe String)
    pass    <- lookup cfg "pass"    :: IO (Maybe String)

    mytardisDir <- lookup cfg "mytardis_directory" :: IO (Maybe String)
    let mytardisDir' = if isNothing mytardisDir then "/imagetrove" else fromJust mytardisDir

    hSetBuffering stdin NoBuffering

    return $ case (user, pass) of
        (Just user', Just pass') -> Just $ defaultMyTardisOptions host user' pass' "http://127.0.0.1:8443" mytardisDir' debug
        _                        -> Nothing

readInstrumentConfigs
  :: FilePath
     -> IO [(String, Maybe String, String, [String], String, String, String, String, String, String, FilePath)]
readInstrumentConfigs f = do
    cfg <- load [Required f]

    instruments <- lookup cfg "instruments" :: IO (Maybe [String])

    case instruments of
        Nothing -> error $ "No instruments specified in configuration file: " ++ f
        Just instruments' -> mapM (readInstrumentConfig cfg . T.pack) instruments'

readInstrumentConfig
  :: Config
       -> T.Text
       -> IO (String, Maybe String, String, [String], String, String, String, String, String, String, FilePath)
readInstrumentConfig cfg instrumentName = do
    topLevelDirectory                   <- lookup cfg (instrumentName `T.append` ".top_level_directory")
    subdirectory                        <- lookup cfg (instrumentName `T.append` ".subdirectory")
    processedDirectory                  <- lookup cfg (instrumentName `T.append` ".processed_directory")

    defaultOperators                    <- lookup cfg (instrumentName `T.append` ".default_operators")

    defaultInstitutionName              <- lookup cfg (instrumentName `T.append` ".default_institution_name")
    defaultInstitutionalDepartmentName  <- lookup cfg (instrumentName `T.append` ".default_institutional_department_name")
    defaultInstitutionalAddress         <- lookup cfg (instrumentName `T.append` ".default_institutional_address")

    schemaExperiment <- lookup cfg (instrumentName `T.append` ".schema_experiment")
    schemaDataset    <- lookup cfg (instrumentName `T.append` ".schema_dataset")
    schemaFile       <- lookup cfg (instrumentName `T.append` ".schema_file")

    tempDirectory    <- fromMaybe "/tmp" <$> lookup cfg (instrumentName `T.append` ".temp_directory")

    when (isNothing topLevelDirectory)  $ error $ "Bad/missing 'top_level_directory' field for "       ++ T.unpack instrumentName
    when (isNothing processedDirectory) $ error $ "Bad/missing 'processed_directory' field for "       ++ T.unpack instrumentName

    when (isNothing defaultOperators)                    $ error $ "Bad/missing 'default_operators"                     ++ T.unpack instrumentName

    when (isNothing defaultInstitutionName)              $ error $ "Bad/missing 'default_institution_name"              ++ T.unpack instrumentName
    when (isNothing defaultInstitutionalDepartmentName)  $ error $ "Bad/missing 'default_institutional_department_name" ++ T.unpack instrumentName
    when (isNothing defaultInstitutionalAddress)         $ error $ "Bad/missing 'default_institutional_address"         ++ T.unpack instrumentName

    when (isNothing schemaExperiment) $ error $ "Bad/missing 'schema_experiment' field for " ++ T.unpack instrumentName
    when (isNothing schemaDataset)    $ error $ "Bad/missing 'schema_dataset' field for "    ++ T.unpack instrumentName
    when (isNothing schemaFile)       $ error $ "Bad/missing 'schema_file' field for "       ++ T.unpack instrumentName

    case ( topLevelDirectory, processedDirectory, defaultOperators , defaultInstitutionName , defaultInstitutionalDepartmentName , defaultInstitutionalAddress , schemaExperiment , schemaDataset , schemaFile) of
        ( Just topLevelDirectory', Just processedDirectory', Just defaultOperators' , Just defaultInstitutionName' , Just defaultInstitutionalDepartmentName' , Just defaultInstitutionalAddress' , Just schemaExperiment' , Just schemaDataset' , Just schemaFile') -> return (topLevelDirectory', subdirectory, processedDirectory', defaultOperators', defaultInstitutionName', defaultInstitutionalDepartmentName', defaultInstitutionalAddress', schemaExperiment', schemaDataset', schemaFile', tempDirectory)
        _ -> error "Error: unhandled case in readInstrumentConfig. Report this bug."

-- | Is this a directory?
isDir :: FilePath -> IO Bool
isDir dir = isDirectory <$> getFileStatus dir

-- | Get the contents of a directory.
getStuff :: FilePath -> IO (Either String [FilePath])
getStuff dir = catch (Right . map (dir </>) . filter (not . (`elem` [".", ".."])) <$> getDirectoryContents dir)
                     (\e -> return $ Left $ show (e :: IOException))

-- | Get directories in the directory.
getDirs :: FilePath -> IO (Either String [FilePath])
getDirs dir = do
    stuff <- getStuff dir
    case stuff of Right stuffs -> filterM isDir stuffs >>= (return . Right)
                  Left err     -> return $ Left err

-- | Test if an *absolute* path is the top level of a Bruker experiment directory.
isBrukerDirectory :: FilePath -> IO Bool
isBrukerDirectory dir = fileExist (dir </> "subject")

-- | A directory is considered stable if it hasn't been accessed or modified
-- for at least 5 minutes.
isStable :: FilePath -> IO (Either String Bool)
isStable dir = catch (Right <$> isStable' dir)
                     (\e -> return $ Left $ show (e :: IOException))
  where
    isStable' dir = do
        now        <- getCurrentTime
        lastChange <- getLastChangedTime dir

        let diff = diffUTCTime now lastChange

        return $ diff > (5*60)

identifyBrukerExperiment :: String -> String -> String -> String -> [String] -> FilePath -> IO (Either String IdentifiedExperiment)
identifyBrukerExperiment schemaExperiment institution institutionalDepartmentName institutionAddress operators dir = do
    projectID <- readProjectID dir

    case projectID of

        Left pidErr     -> return $ Left $ "Error: could not find project ID in " ++ dir ++ "; " ++ pidErr

        Right projectID' -> do let m1 = [ ("InstitutionName",             institution)
                                        , ("InstitutionalDepartmentName", institutionalDepartmentName)
                                        , ("InstitutionAddress",          institutionAddress)
                                        , ("Project",                     show projectID')
                                        ]

                                   m2 = [("Operator", intercalate " " operators)]

                                   m = M.fromList $ m1 ++ m2

                               return $ Right $ IdentifiedExperiment desc institution title [(schemaExperiment, m)]

  where
    desc            = "" -- FIXME What should this be?
    title           = last $ splitDirectories dir

getLastChangedTime :: FilePath -> IO UTCTime
getLastChangedTime x = do
    stat <- getFileStatus x

    let ctimeToUTC = \t -> posixSecondsToUTCTime (realToFrac t :: POSIXTime)

    let atime = ctimeToUTC $ accessTime       stat
        mtime = ctimeToUTC $ modificationTime stat

    return $ max atime mtime

tarBrukerDirectory :: FilePath -> FilePath -> IO (Either String FilePath)
tarBrukerDirectory tempDir dir = do
    let up      = joinPath . reverse . tail . reverse . splitDirectories $ dir -- FIXME Can break.
        title   = last $ splitDirectories dir                                  -- FIXME last is dangerous if empty path

    oldCwd <- getCurrentDirectory

    print ("up", up)
    print ("title", title)

    setCurrentDirectory up -- FIXME catch exception?

    let tarball = tempDir </> (title ++ ".tar.gz")

    let cmd     = "tar"
        args    = ["zcf", tarball, title]

    putStrLn $ "Running: " ++ show (cmd, args)
    tarballResult <- runShellCommand cmd args

    setCurrentDirectory oldCwd

    case tarballResult of
        Left err    -> return $ Left $ "Error while creating tarball of " ++ dir ++ ": " ++ err
        Right _     -> return $ Right tarball

pvconvScript = "/data/home/uqchamal/perl5/bin/pvconv.pl" -- FIXME make parameter? Why is the path broken?

isNumber :: String -> Bool
isNumber = all isDigit

brukerToMinc :: FilePath -> FilePath -> IO FilePath
brukerToMinc tempDir dir = do
    let up      = joinPath . reverse . tail . reverse . splitDirectories $ dir -- FIXME Can break.
        title   = last $ splitDirectories dir                                  -- FIXME last is dangerous if empty path

    oldCwd <- getCurrentDirectory

    print ("up", up)
    print ("title", title)

    setCurrentDirectory up -- FIXME catch exception?
    putStrLn $ "brukerToMinc: working in directory: " ++ up

    -- The series directories are directory names that are numbers. We have to process
    -- them individually because pvconv blows up and gives up if it can't handle one of them.
    series <- (filter isNumber) <$> getDirectoryContents "."

    putStrLn $ "brukerToMinc: found these series directories: " ++ show series

    -- FIXME No good error handling here!

    forM_ series $ \s -> do
        let cmd     = pvconvScript
            args    = ["-series", s, title, "-verbose", "-outtype", "minc", "-outdir", tempDir]

        putStrLn $ "Running: " ++ show (cmd, args)
        _ <- runShellCommand' cmd args

        setCurrentDirectory oldCwd

    return tempDir

mincFilesInDir :: FilePath -> IO [FilePath]
mincFilesInDir dir = getDirectoryContents dir >>= (return . (map (dir </>)) . filter (isSuffixOf ".mnc"))

-- copied from MainDicom.hs
identifyDatasetFile :: RestDataset -> String -> String -> Integer -> [(String, M.Map String String)] -> IdentifiedFile
identifyDatasetFile rds filepath md5sum size metadata = IdentifiedFile
                                        (dsiResourceURI rds)
                                        filepath
                                        md5sum
                                        size
                                        metadata

pushFiles :: String -> (RestDataset -> String -> String -> Integer -> [(String, Map.Map String String)] -> IdentifiedFile) -> RestDataset -> [FilePath] -> ReaderT MyTardisConfig IO (Either [(FilePath, A.Result RestDatasetFile)] [(FilePath, A.Result RestDatasetFile)])
pushFiles schemaFile identifyDatasetFile ds' files = do
    results <- forM files $ \f -> uploadFileBasic schemaFile identifyDatasetFile ds' f []

    let filesAndResults = zip files results

        goodUpload (_, A.Success _) = True
        goodUpload _                = False

        goodUploads = filter goodUpload         filesAndResults
        badUploads  = filter (not . goodUpload) filesAndResults

    return $ case badUploads of
        []  -> Right $ goodUploads -- no bad uploads
        _   -> Left $ badUploads   -- some failed uploads

data BrukerUploadError = BrukerTarballError             String
                       | BrukerIdentifyError            String
                       | BrukerCreateExperimentError    String
                       | BrukerCreateDatasetError       String
                       | BrukerCreateFilesError         [(FilePath, A.Result RestDatasetFile)]
                       | BrukerCreateProjectGroupError  String
                       | BrukerNoProjectID              String
                       | BrukerFileSuccess              [(FilePath, A.Result RestDatasetFile)]
                       deriving (Show)

-- Parse the project ID.
stage1 :: FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError String)
stage1 dir = do
    projectID <- liftIO $ readProjectID dir

    return $ case projectID of
       Left  pidErr     -> Left  $ BrukerNoProjectID pidErr
       Right projectID' -> Right $ projectID'

-- Create the project group.
stage2 :: String -> ReaderT MyTardisConfig IO (Either BrukerUploadError RestGroup)
stage2 projectID' = do
    projectResult <- getOrCreateGroup $ "Project " ++ projectID'
    return $ case projectResult of
        A.Error   projErr       -> Left  $ BrukerCreateProjectGroupError projErr
        A.Success projResult'   -> Right $ projResult'

-- Make tarball of Bruker experiment directory.
stage3 :: FilePath -> String -> String -> String -> FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError FilePath)
stage3 tarballTempDir schemaExperiment schemaDataset schemaFile dir = do
    tarball <- liftIO $ tarBrukerDirectory tarballTempDir dir

    return $ case tarball of
        Left err        -> Left  $ BrukerTarballError err
        Right tarball'  -> Right $ tarball'

-- Convert to MINC files.
stage3a :: FilePath -> String -> String -> String -> FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError [FilePath])
stage3a mincTempDir schemaExperiment schemaDataset schemaFile dir = do
    mincDir     <- liftIO $ brukerToMinc mincTempDir dir
    mincFiles   <- liftIO $ mincFilesInDir mincDir
    return $ Right mincFiles

-- Create thumbnails of MINC files.
stage3b mincFiles = do
    thumbnails <- liftIO $ forM mincFiles createMincThumbnail

    -- We don't mind if thumbnails fail...
    forM_ (lefts thumbnails) $ \terr -> liftIO $ putStrLn $ "Error when creating thumbnail: " ++ terr

    return $ Right $ rights thumbnails

-- Identify the experiment (preparation for creating an actual experiment).
stage4 :: String -> String -> String -> String -> [String] -> FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError IdentifiedExperiment)
stage4 schemaExperiment institution institutionalDepartmentName institutionAddress operators dir = do
    ie <- liftIO $ identifyBrukerExperiment schemaExperiment institution institutionalDepartmentName institutionAddress operators dir

    return $ case ie of
      Left ieErr    -> Left  $ BrukerIdentifyError ieErr
      Right ie'     -> Right $ ie'

-- Create the experiment.
stage5 :: IdentifiedExperiment -> ReaderT MyTardisConfig IO (Either BrukerUploadError RestExperiment)
stage5 ie' = do
    e <- createExperiment ie'
    return $ case e of
        A.Error   eerr  -> Left  $ BrukerCreateExperimentError eerr
        A.Success e'    -> Right $ e'

-- Identify the dataset.
stage6 :: String -> RestExperiment -> ReaderT MyTardisConfig IO (Either BrukerUploadError RestDataset)
stage6 schemaDataset e' = do
    let ids = IdentifiedDataset
                "Bruker dataset"
                [eiResourceURI e']
                [(schemaDataset, Map.empty)]
    ds <- createDataset ids

    return $ case ds of
        A.Error   derr  -> Left  $ BrukerCreateDatasetError derr
        A.Success ds'   -> Right $ ds'

-- Push all the files (tarball, mincs, thumbnails).
stage7 :: String -> [FilePath] -> RestDataset -> ReaderT MyTardisConfig IO (Either BrukerUploadError [(FilePath, A.Result RestDatasetFile)])
stage7 schemaFile files ds' = do
    fileResults <- pushFiles schemaFile identifyDatasetFile ds' files
    return $ case fileResults of
        Right fileResults' -> Right $ fileResults'
        Left  fileErrors   -> Left  $ BrukerCreateFilesError fileErrors

joinEither :: Either a (Either a b) -> Either a b
joinEither (Left a)          = Left a
joinEither (Right (Right b)) = Right b
joinEither (Right (Left  a)) = Left a

removeRecursively :: FilePath -> IO (Either String FilePath)
removeRecursively dir = catch (removeRecursiveSafely dir >> return (Right dir))
                              (\e -> return $ Left $ show (e :: IOException))

-- FIXME expects absolute paths?
safeMove :: FilePath -> FilePath -> FilePath -> IO (Either String FilePath)
safeMove topDir dir destination = do
    -- e.g. topDir == '/opt/imagetrove/incoming'
    --      dir    == '/opt/imagetrove/incoming/uqchamal/experiment1'

    liftIO $ do putStrLn $ "saveMove: topDir: "      ++ topDir
                putStrLn $ "saveMove: dir: "         ++ dir
                putStrLn $ "saveMove: destination: " ++ destination

    let part = joinPath $ drop (length $ splitPath topDir) (splitPath dir) -- e.g. 'uqchamal/experiment1'
        result0 = destination </> part

    liftIO $ putStrLn $ "Attempting to move " ++ dir ++ " to " ++ result0

    -- topDir must be a prefix of dir.
    let result1 = if topDir `isPrefixOf` dir
                        then Right result0
                        else Left $ topDir ++ " is not prefix of " ++ dir
    print result1

    -- dir must be stable
    s <- isStable dir
    let result2 = case s of Right True  -> result1
                            Right False -> Left $ "Error: source directory " ++ dir ++ " is not stable."
                            Left  err   -> Left err
    print result2

    -- destination </> part must not exist
    let fullDestination = destination </> part
        g = \r d b -> if not b then r else Left $ "Error: destination " ++ d ++ " exists."
    result3 <- (g result2 fullDestination) <$> fileExist fullDestination
    print ("fullDestination", fullDestination)
    print ("result3", result3)

    target <- catch (createDirectoryIfMissing True fullDestination >> return (Right ()))
                    (\e -> return $ Left $ show (e :: IOException))
    print target

    let cmd  = "rsync"
        args = [ "-a"
               , (dropTrailingPathSeparator dir)             ++ "/" -- trailing slash for rsync
               , (dropTrailingPathSeparator fullDestination) ++ "/" -- trailing slash for rsync
               ]

    print (cmd, args)

    x <- runShellCommand cmd args
    print x

    let result4 = case x of err@(Left _) -> err
                            Right _      -> result3
    print result4

    finalResult <- case result4 of err@(Left _) -> return err
                                   Right _      -> removeRecursively dir

    return finalResult



processBrukerExperiment' :: (FilePath, Maybe FilePath, FilePath, [String], String, String, String, String, String, String, FilePath) -> FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError [(FilePath, A.Result RestDatasetFile)])
processBrukerExperiment' instrumentConfig dir = do
    let (topLevelDirectory, subdirectory, processedDirectory, operators, institution, institutionalDepartmentName, institutionAddress, schemaExperiment, schemaDataset, schemaFile, tmp) = instrumentConfig

    tarballTempDir <- liftIO $ createTempDirectory tmp "bruker_experiment"
    mincTempDir    <- liftIO $ createTempDirectory tmp "bruker_to_minc"

    projectID       <- stage1 dir
    projectResult   <- traverse stage2 projectID

    schemas <- createSchemasIfMissing (schemaExperiment, schemaDataset, schemaFile)

    tarball         <- stage3  tarballTempDir schemaExperiment schemaDataset schemaFile dir
    mincFiles       <- stage3a mincTempDir schemaExperiment schemaDataset schemaFile dir
    thumbnails      <- joinEither <$> traverse stage3b mincFiles

    ie              <- stage4 schemaExperiment institution institutionalDepartmentName institutionAddress operators dir
    e               <- joinEither <$> traverse stage5 ie

    ds              <- joinEither <$> traverse (stage6 schemaDataset) e

    let (+++) a b c = a ++ b ++ c
        files = (+++) <$> (pure <$> tarball) <*> mincFiles <*> thumbnails

    -- If the tarball isn't there then we give up. Other files,
    -- such as MINCs and thumbnails, are optional.

    finalResult <- case (tarball, files) of
        (Left terr, _           )   -> return $ Left terr
        (Right   _, Right files')   -> joinEither <$> traverse (stage7 schemaFile files') ds

    debug <- mytardisDebug <$> ask

    when (not debug) $
        forM_ [tarballTempDir, mincTempDir] $ \x -> liftIO $
            do putStrLn $ "Removing temp directory: " ++ x
               removeRecursiveSafely x

    return finalResult

{-
processBrukerExperiment :: (FilePath, Maybe FilePath, [String], String, String, String, String, String, String, FilePath) -> FilePath -> ReaderT MyTardisConfig IO (Either BrukerUploadError (RestExperiment, RestDataset, [(FilePath, A.Result RestDatasetFile)]))
processBrukerExperiment instrumentConfig dir = do

    let (topLevelDirectory, subdirectory, operators, institution, institutionalDepartmentName, institutionAddress, schemaExperiment, schemaDataset, schemaFile, tmp) = instrumentConfig

    tarballTempDir <- liftIO $ createTempDirectory tmp "bruker_experiment"
    mincTempDir    <- liftIO $ createTempDirectory tmp "bruker_to_minc"

    projectID <- liftIO $ readProjectID dir

    case projectID of
       Left pidErr -> return $ Left $ BrukerNoProjectID pidErr
       Right projectID' -> do projectResult <- getOrCreateGroup $ "Project " ++ projectID'
                              case projectResult of A.Error   projErr        -> return $ Left $ BrukerCreateProjectGroupError projErr
                                                    A.Success _ ->  do tarball <- liftIO $ tarBrukerDirectory tarballTempDir dir
                                                                       mincDir <- liftIO $ brukerToMinc       mincTempDir    dir

                                                                       schemas <- createSchemasIfMissing (schemaExperiment, schemaDataset, schemaFile)

                                                                       case tarball of
                                                                           Left err        -> return $ Left $ BrukerTarballError err
                                                                           Right tarball'  -> do mincFiles  <- liftIO $ mincFilesInDir mincDir
                                                                                                 thumbnails <- liftIO $ forM mincFiles createMincThumbnail

                                                                                                 forM_ (lefts thumbnails) $ \terr -> liftIO $ putStrLn $ "Error when creating thumbnail: " ++ terr

                                                                                                 ie <- liftIO $ identifyBrukerExperiment schemaExperiment institution institutionalDepartmentName institutionAddress operators dir

                                                                                                 case ie of
                                                                                                   Left ieErr  -> return $ Left $ BrukerIdentifyError ieErr
                                                                                                   Right ie'   -> do e <- createExperiment ie'

                                                                                                                     case e of
                                                                                                                       A.Error eerr -> return $ Left $ BrukerCreateExperimentError eerr
                                                                                                                       A.Success e' -> do let ids = IdentifiedDataset
                                                                                                                                                       "Bruker dataset"
                                                                                                                                                       [eiResourceURI e']
                                                                                                                                                       [(schemaDataset, Map.empty)]
                                                                                                                                          ds <- createDataset ids

                                                                                                                                          case ds of
                                                                                                                                               A.Error derr  -> return $ Left $ BrukerCreateDatasetError derr
                                                                                                                                               A.Success ds' -> do fileResults <- pushFiles schemaFile identifyDatasetFile ds' ([tarball'] ++ mincFiles ++ rights thumbnails)
                                                                                                                                                                   case fileResults of
                                                                                                                                                                       Right fileResults' -> return $ Right $ (e', ds', fileResults')
                                                                                                                                                                       Left  fileErrors   -> return $ Left  $ BrukerCreateFilesError fileErrors

-}

imageTroveMain :: IO ()
imageTroveMain = do
    opts' <- execParser opts

    let host     = fromMaybe "http://localhost:8000" $ optHost opts'
        config   = optConfigFile opts'
        debug    = optDebug opts'

    mytardisOpts <- getConfig host config debug

    case mytardisOpts of
        (Just mytardisOpts') -> runReaderT (dostuff config) mytardisOpts'
        _                    -> error $ "Could not read config file: " ++ config

  where
    opts = info (helper <*> pUploaderOptions ) (fullDesc <> header "imagetrove-bruker-uploader - upload Bruker data to a MyTARDIS server" )


getTopLevelDirectory  (topLevelDirectory, _,            _,                  _, _, _, _, _, _, _, _) = topLevelDirectory
getSubdirectory       (_,                 subdirectory, _,                  _, _, _, _, _, _, _, _) = subdirectory
getProcessedDirectory (_,                 _,            processedDirectory, _, _, _, _, _, _, _, _)    = processedDirectory

doInstrument iconfig = do

    let topDir          = getTopLevelDirectory  iconfig
        subDir          = getSubdirectory       iconfig
        processedDir    = getProcessedDirectory iconfig

    liftIO $ putStrLn $ "doInstrument: topDir: "       ++ topDir
    liftIO $ putStrLn $ "doInstrument: subDir: "       ++ show subDir
    liftIO $ putStrLn $ "doInstrument: processedDir: " ++ processedDir

    -- topDir like: /data/home/uqchamal/mounts/bio94t/opt/imagetrove/archive_PV5.1

    -- Each user has a directory under the top directory.
    userDirs <- liftIO $ getDirs topDir -- ["uqchamal", "uqytesir", ...]

    possibleExperimentDirs <- liftIO $ case (userDirs, subDir) of
        (Right userDirs', Just subDir')     -> forM [topDir </> ud </> subDir' | ud <- userDirs'] getDirs >>= (return . concat . rights)
        (Right userDirs', Nothing)          -> forM [topDir </> ud             | ud <- userDirs'] getDirs >>= (return . concat . rights)
        _                                   -> return []

    let foo (Right True) = True
        foo _            = False

    stable <- liftIO $ filterM ((fmap foo) . isStable) possibleExperimentDirs

    liftIO $ putStrLn $ "doInstrument: stable: " ++ show stable

    forM_ stable $ \dir -> doExperiment iconfig dir topDir subDir processedDir

doExperiment iconfig dir topDir subDir processedDir = do
    liftIO $ putStrLn $ "Processing: " ++ dir
    x <- processBrukerExperiment' iconfig dir
    case x of
      Left (BrukerTarballError             s)            -> liftIO $ putStrLn $ "Error creating tarball: " ++ s
      Left (BrukerIdentifyError            s)            -> liftIO $ putStrLn $ "Error identifying experiment: " ++ s
      Left (BrukerCreateExperimentError    s)            -> liftIO $ putStrLn $ "Error creating experiment: " ++ s
      Left (BrukerCreateDatasetError       s)            -> liftIO $ putStrLn $ "Error creating dataset: " ++ s
      Left (BrukerCreateFilesError         fileErrors)   -> liftIO $ putStrLn $ "Error creating file(s): " ++ show fileErrors
      Left (BrukerCreateProjectGroupError  s)            -> liftIO $ putStrLn $ "Error creating project group: " ++ s
      Left (BrukerNoProjectID s)                         -> liftIO $ putStrLn $ "Error: could not find project ID in subject file: " ++ s
      Right _ -> liftIO $ do putStrLn $ "Processed: " ++ dir
                             putStrLn $ "Now attempting safe copy to move to processed directory."
                             y <- safeMove topDir dir processedDir
                             case y of Right _      -> putStrLn $ "Successfully moved experiment to " ++ processedDir
                                       err@(Left _) -> putStrLn $ "Error while moving experiment to processed directory: " ++ show err

dostuff configFile = do
    origDir <- liftIO getCurrentDirectory
    forever $ do liftIO $ setCurrentDirectory origDir

                 -- Main work:
                 instrumentConfigs <- liftIO $ readInstrumentConfigs configFile
                 forM_ instrumentConfigs doInstrument

                 -- Snooze:
                 let sleepMinutes = 1
                 liftIO $ printf "Sleeping for %d minutes...\n" sleepMinutes
                 liftIO $ threadDelay $ sleepMinutes * (60 * 10^6)
