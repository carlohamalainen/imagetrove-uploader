{-# LANGUAGE DeriveDataTypeable, ScopedTypeVariables #-}

module Network.ImageTrove.Utils where

import Control.Monad.Catch
import Data.Typeable

import Control.Applicative ((<$>))
import Control.Monad (filterM, liftM)

import qualified Pipes.ByteString as B

import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Internal (c2w, w2c)

import Crypto.Hash.SHA1 (hashlazy)
import Text.Printf (printf)

import qualified Data.ByteString as Strict

import Data.Either ()
import System.IO
import System.Process
import System.Process.Streaming

import System.Exit

import System.Directory
import System.FilePath
import System.Posix.Files

import Control.Exception (IOException(..))

import Network.Wreq

import Data.Time.Clock (addUTCTime, getCurrentTime, diffUTCTime, UTCTime(..), NominalDiffTime(..))
import Data.Time.LocalTime (getZonedTime, utcToZonedTime, zonedTimeToUTC, TimeZone(..), ZonedTime(..))
import Data.Time.Format (parseTime)
import Data.Time.Clock.POSIX
import System.Locale (defaultTimeLocale)

data ImagetroveException = AnonymizationException   String
                         | ShellCommandException    String
                         | EmptySeriesException     String
                         | MetadataMissingException (Maybe String, Maybe String, Maybe String, Maybe String)
                         | OtherImagetroveException String
    deriving (Show, Typeable)

instance Exception ImagetroveException


_runShellCommand :: FilePath -> FilePath -> [String] -> IO String
_runShellCommand cwd cmd args = do
    x <- runShellCommand cwd cmd args
    case x of
        Left e    -> throwM $ ShellCommandException e
        Right out -> return out

runShellCommand :: FilePath -> FilePath -> [String] -> IO (Either String String)
runShellCommand cwd cmd args = do
    -- putStrLn $ "runShellCommand: " ++ show (cwd, cmd, args)

    (exitCode, (stdOut, stdErr)) <- execute (pipeoe (fromFold B.toLazyM) (fromFold B.toLazyM)) ((proc cmd args) { cwd = Just cwd })

    return $ case exitCode of
        ExitSuccess   -> Right $ map w2c $ BS.unpack $ BSL.toStrict stdOut
        ExitFailure e -> Left $ "runShellCommand: exit status " ++ show e ++ " with stdErr: " ++ (map w2c $ BS.unpack $ BSL.toStrict $ stdErr)

-- | Run the shell command and return some output, ignoring the
-- return code. This is useful for badly behaved utilities.
runShellCommand' :: FilePath -> FilePath -> [String] -> IO String
runShellCommand' cwd cmd args = do
    (exitCode, (stdOut, stdErr)) <- execute (pipeoe (fromFold B.toLazyM) (fromFold B.toLazyM)) ((proc cmd args) { cwd = Just cwd })

    return $ case exitCode of
        ExitSuccess   -> map w2c $ BS.unpack $ BSL.toStrict stdOut
        ExitFailure e -> "runShellCommand: exit status " ++ show e ++ " with stdErr: " ++ (map w2c $ BS.unpack $ BSL.toStrict $ stdErr)

computeChecksum :: FilePath -> IO (Either String String)
computeChecksum fileName = do
    cwd <- getCurrentDirectory
    liftM (head . words) <$> runShellCommand cwd "md5sum" [fileName]

sha256 :: String -> String
sha256 s = (toHex . hashlazy . BSL.pack) (map c2w s)
  where
    toHex :: Strict.ByteString -> String
    toHex bytes = Strict.unpack bytes >>= printf "%02x"

-- Wrapper around various Wreq functions.

getWithE opts x = (Right <$> getWith opts x) `catch` handler
  where
    handler (e :: SomeException) = return $ Left $ "Error in getWithE: " ++ show e

putWithE opts x v = (Right <$> putWith opts x v) `catch` handler
  where
    handler (e :: SomeException) = return $ Left $ "Error in putWithE: " ++ show e

postWithE opts x v = (Right <$> postWith opts x v) `catch` handler
  where
    handler (e :: SomeException) = return $ Left $ "Error in postWithE: " ++ show e

deleteWithE opts x = (Right <$> deleteWith opts x) `catch` handler
  where
    handler (e :: SomeException) = return $ Left $ "Error in deleteWithE: " ++ show e



joinEither :: Either a (Either a b) -> Either a b
joinEither (Left a)          = Left a
joinEither (Right (Right b)) = Right b
joinEither (Right (Left  a)) = Left a


-- | Is this a directory?
isDir :: FilePath -> IO Bool
isDir dir = isDirectory <$> getFileStatus dir

-- | Get the contents of a directory.
getStuff :: FilePath -> IO (Either String [FilePath])
getStuff dir = catch (Right <$> getStuff' dir)
                     (\e -> return $ Left $ show (e :: IOException))

getStuff' :: FilePath -> IO [FilePath]
getStuff' dir = map (dir </>) . filter (not . (`elem` [".", ".."])) <$> getDirectoryContents dir

-- | Get directories in the directory.
getDirs :: FilePath -> IO (Either String [FilePath])
getDirs dir = do
    stuff <- getStuff dir
    case stuff of Right stuffs -> filterM isDir stuffs >>= (return . Right)
                  Left err     -> return $ Left err

getDirs' :: FilePath -> IO [FilePath]
getDirs' dir = getStuff' dir >>= filterM isDir

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

getLastChangedTime :: FilePath -> IO UTCTime
getLastChangedTime x = do
    stat <- getFileStatus x

    let ctimeToUTC = \t -> posixSecondsToUTCTime (realToFrac t :: POSIXTime)

    let atime = ctimeToUTC $ accessTime       stat
        mtime = ctimeToUTC $ modificationTime stat

    return $ max atime mtime

isDCMTKStudyStable :: FilePath -> IO Bool
isDCMTKStudyStable dir = do
    now        <- getCurrentTime
    lastChange <- getStudyMTime dir

    let diff = diffUTCTime now lastChange

    return $ diff > (5*60)

getStudyMTime :: FilePath -> IO UTCTime
getStudyMTime dir = do
    files <- getStuff' dir
    mtimes <- mapM getMTime (dir:files)
    return $ maximum mtimes
  where
    getMTime f = (ctimeToUTC . modificationTime) <$> getFileStatus f
    ctimeToUTC = \t -> posixSecondsToUTCTime (realToFrac t :: POSIXTime)


getStudyDirName :: [FilePath] -> String
getStudyDirName [] = throwM $ EmptySeriesException "No files in series in getStudyDirName."
getStudyDirName (f:_) = (head . drop 1 . reverse . splitDirectories) f

getStableWithNameAndTime :: NominalDiffTime -> FilePath -> IO [(FilePath, String, ZonedTime)]
getStableWithNameAndTime nrMinutes dir = do
    studyDirs   <- getDirs' dir
    studyMTimes <- mapM getStudyMTime studyDirs

    now <- getCurrentTime
    ZonedTime _ tz <- getZonedTime

    let studyDirNames = map studyDirName studyDirs
        diffs = map (diffUTCTime now) studyMTimes
        stuff = zip3 studyDirs studyMTimes diffs
        stable = filter (\(_,_, diff) -> diff > (nrMinutes*60)) stuff

        result  = zip stable studyDirNames                                                      :: [((FilePath, UTCTime, NominalDiffTime), String)]
        result' = map (\((fp, mtime, _), dname) -> (fp, dname, utcToZonedTime tz mtime)) result :: [(FilePath, String, ZonedTime)]

    return result'

  where
    studyDirName :: FilePath -> String
    studyDirName = (head . drop 1 . reverse . splitDirectories)

