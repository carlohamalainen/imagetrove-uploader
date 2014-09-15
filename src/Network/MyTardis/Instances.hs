-- | Instances of 'Foldable' and 'Traversable' for 'Result'.

module Network.MyTardis.Instances where

import Control.Applicative
import Data.Aeson (Result(..))
import Data.Traversable
import Data.Foldable

-- | For the instance of 'Foldable' for 'Result'.
foldrResult _ z (Error _) = z
foldrResult f z (Success x) = f x z

-- | For the instance of 'Foldable' for 'Result'.
foldlResult _ z (Error _) = z
foldlResult f z (Success x) = f z x

-- | For the instance of 'Traversable' for 'Result'.
traverseResult _ (Error e) = pure $ Error e
traverseResult f (Success x) = Success <$> f x

instance Foldable Result where
    -- This gives an "Orphan instance" warning. Is that a problem?
    foldr = foldrResult
    foldl = foldlResult

instance Traversable Result where
    -- This gives an "Orphan instance" warning. Is that a problem?
    traverse = traverseResult
