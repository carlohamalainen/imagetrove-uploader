module Main where

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

import Network.ImageTrove.Main

main = dicomMain
