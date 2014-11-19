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

runShellCommand :: FilePath -> [String] -> IO (Either String String)
runShellCommand cmd args = do
    x <- simpleSafeExecute (pipeoe $ separated (surely B.toLazyM) (surely B.toLazyM)) (proc cmd args)

    return $ case x of Left e            -> Left e
                       Right (stdOut, _) -> Right $ map w2c $ BS.unpack $ BSL.toStrict stdOut

computeChecksum :: FilePath -> IO (Either String String)
computeChecksum fileName = liftM (head . words) <$> runShellCommand "md5sum" [fileName]

sha256 :: String -> String
sha256 s = (toHex . hashlazy . BSL.pack) (map c2w s)
  where
    toHex :: Strict.ByteString -> String
    toHex bytes = Strict.unpack bytes >>= printf "%02x"


