{-# LANGUAGE ScopedTypeVariables #-}

module Network.ImageTrove.Utils where

import Control.Applicative ((<$>))
import Control.Monad (liftM)

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
import System.Directory (getCurrentDirectory)

import Control.Exception
import Network.Wreq

runShellCommand :: FilePath -> FilePath -> [String] -> IO (Either String String)
runShellCommand cwd cmd args = do
    putStrLn $ "runShellCommand: " ++ show (cwd, cmd, args)

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
