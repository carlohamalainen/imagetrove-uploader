{-# LANGUAGE RankNTypes #-}

module Network.ImageTrove.Bruker (readProjectID) where

import qualified Control.Monad as CM
import System.FilePath ((</>))
import Data.Functor.Identity
import Text.Parsec
import Data.Maybe
import Data.Either

import Control.Exception.Base (catch, IOException(..))

-- | CAI project ID in a Bruker subject file.
parseCAIProjectID :: forall u. ParsecT String u Identity String
parseCAIProjectID = do
    _ <- string "CAI:"
    d1 <- digit
    d2 <- digit
    d3 <- digit
    d4 <- digit
    d5 <- digit

    return [d1, d2, d3, d4, d5]

parseProjectIdInLine = parseCAIProjectID <|> (anyToken >> parseProjectIdInLine)

readProjectID :: FilePath -> IO (Either String String)
readProjectID dir = do
    matches <- catch ((Right . rights . (map (parse parseProjectIdInLine "(parseProjectIdInLine )")) . lines) `CM.fmap` readFile (dir </> "subject"))
                     (\e -> return $ Left $ show (e :: IOException))

    return $ case matches of
        Right [projectID]   -> Right projectID
        Right []            -> Left $ "No project ID found in " ++ dir
        Right huh           -> Left $ "Multiple project IDs " ++ show huh ++ " in " ++ dir
        err                 -> Left $ "Couldn't parse project ID in " ++ dir ++ " with error: " ++ show err
