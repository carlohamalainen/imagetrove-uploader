{-# LANGUAGE RankNTypes #-}

module Network.ImageTrove.Bruker (isBrukerDirectoryName) where

import Data.Functor.Identity
import Text.Parsec

-- | A valid Bruker directory name looks like this: SGEL3A11tp3.sz1
parseBrukerDirectoryName :: forall u. ParsecT String u Identity String
parseBrukerDirectoryName = do
    x <- many (noneOf ".")

    _ <- char '.'

    c1 <- alphaNum
    c2 <- alphaNum

    return $ x ++ ['.', c1, c2]

isBrukerDirectoryName :: FilePath -> Bool
isBrukerDirectoryName dir = case parse parseBrukerDirectoryName "(parseBrucker)" dir of
                                Left _      -> False
                                Right _     -> True
