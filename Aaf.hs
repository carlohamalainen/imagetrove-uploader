module Main where

import Text.CSV

import Control.Monad (forM_)

import Data.Aeson (Result(..))

import Data.Char

import Control.Monad.Reader

import qualified Data.ByteString as B
import Data.ByteString.Internal (c2w)

import Text.Email.Validate (isValid)

import Network.MyTardis.API (addUserToGroup, getOrCreateUser, getOrCreateGroup)

import Network.ImageTrove.Main (getConfig)

isValid' :: String -> Bool
isValid' e = isValid $ B.pack $ map c2w e

isProjectName :: String -> Bool
isProjectName [] = False
isProjectName p@(x:xs) = niceChars && noStartingSpace && noTrailingSpace
  where     
    niceChars = all (\c -> isAlphaNum c || c == ' ') p
    noStartingSpace = not $ isSpace x
    noTrailingSpace = not $ isSpace $ last p

superuserLine [] = return ()
superuserLine [""] = return ()
superuserLine l@(e:[]) = if isValid' e
                              then do liftIO $ putStrLn $ "Creating superuser: " ++ show e
                                      u <- getOrCreateUser Nothing Nothing e [] True
                                      return ()
                              else liftIO $ putStrLn $ "Invalid email address: " ++ e
superuserLine l = liftIO $ putStrLn $ "Invalid superuser line: " ++ unwords l

projectLine []   = return ()
projectLine [""] = return ()
projectLine l@(p:e:[]) = if isProjectName p && isValid' e
                              then do liftIO $ putStrLn $ show (p, e)

                                      user  <- getOrCreateUser Nothing Nothing e [] False
                                      group <- getOrCreateGroup p

                                      case (user, group) of
                                        (Success user', Success group') -> do addUserToGroup user' group'
                                                                              liftIO $ putStrLn $ "Added " ++ e ++ " to " ++ p
                                        (Error e, Success _) -> error $ "Could not create user "  ++ e ++ ": " ++ show e
                                        (Success _, Error e) -> error $ "Could not create group " ++ p ++ ": " ++ show e
                                        (Error e1, Error e2) -> error $ "Could not create user or group: " ++ show (e1, e2)
                                      return ()
                              else liftIO $ putStrLn $ "Invalid project line: " ++ show l
projectLine x = liftIO $ putStrLn $ "Unknown project line: " ++ show x

dostuff = do
    let filename = "superusers.csv"
    csv <- liftIO $ readFile filename
    case parseCSV filename csv of
        Right csv' -> forM_ csv' superuserLine
        Left e -> liftIO $ putStrLn $ "Error parsing CSV file: " ++ show e

    let filename' = "projects.csv"
    csv <- liftIO $ readFile filename'
    case parseCSV filename' csv of
        Right csv' -> forM_ csv' projectLine
        Left e -> liftIO $ putStrLn $ "Error parsing CSV file: " ++ show e

main = do
    let host = "http://localhost:8000"

    let cf = "acl.conf"

    mytardisOpts <- getConfig host cf Nothing

    case mytardisOpts of
        (Just mytardisOpts') -> runReaderT dostuff mytardisOpts'
        _                    -> error $ "Could not read config file: " ++ cf
