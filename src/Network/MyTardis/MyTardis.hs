{-# LANGUAGE OverloadedStrings, RankNTypes, ScopedTypeVariables #-}

module Network.MyTardis.MyTardis where

import Control.Applicative ((<$>))
import Control.Lens
import Control.Monad (join, forM_, when, liftM)

import Control.Monad.Reader
import Control.Monad.Identity
import Control.Monad.Trans (liftIO)

import Data.Aeson
import Data.Aeson.Types
import Data.List (isPrefixOf)
import Data.Maybe
import Data.Traversable (traverse)
import Network.HTTP.Client (defaultManagerSettings, managerResponseTimeout)
import Network.Mime
import Network.Wreq


import Safe

import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.ByteString.Lazy   as BL
import qualified Data.ByteString.Char8  as B8

import qualified Data.Text as T
import qualified Data.Map as M

import Network.ImageTrove.Utils (computeChecksum)
import Network.MyTardis.Instances
import Network.MyTardis.Types
import Network.MyTardis.RestTypes

data MyTardisConfig = MyTardisConfig
    { myTardisHost       :: String -- ^ URL to MyTARDIS instance, e.g. \"http://localhost:8000\"
    , myTardisApiBase    :: String -- ^ API base. Should be \"\/api\/v1\"
    , myTardisUser       :: String -- ^ Username.
    , myTardisPass       :: String -- ^ Password.
    , myTardisWreqOpts   :: Network.Wreq.Options -- ^ Options for Wreq.
    }
    deriving Show

-- | Helper to create 'MyTardisConfig' with default values.
defaultMyTardisOptions :: String -- ^ Hostname
                       -> String -- ^ Username
                       -> String -- ^ Password
                       -> MyTardisConfig
defaultMyTardisOptions host user pass = MyTardisConfig host "/api/v1" user pass opts
  where
    opts = defaults & auth .~ basicAuth (B8.pack user) (B8.pack pass)

-- | Wrapper around Wreq's 'postWith' that uses our settings in a 'ReaderT'.
postWith' :: String -> Value -> ReaderT MyTardisConfig IO (Response BL.ByteString)
postWith' x v = do
    MyTardisConfig host apiBase user pass opts <- ask
    liftIO $ postWith opts (host ++ apiBase ++ x) v

putWith' :: String -> Value -> ReaderT MyTardisConfig IO (Response BL.ByteString)
putWith' x v = do
    MyTardisConfig host apiBase user pass opts <- ask
    liftIO $ putWith opts (host ++ apiBase ++ x) v

-- | Create a resource. See 'createExperiment' or similar for how to use.
createResource :: ToJSON b => String -> (String -> ReaderT MyTardisConfig IO (Result a)) -> b -> ReaderT MyTardisConfig IO (Result a)
createResource resourceURL getFn keyValueMap = do
    r <- postWith' resourceURL (toJSON keyValueMap)

    let url = B8.unpack <$> r ^? responseHeader "Location"

    host <- myTardisHost <$> ask

    case url of Just url' -> getFn $ drop (length host) url'
                Nothing   -> return $ Error "Failed to fetch newly created resource."

updateResource  :: ToJSON a => a -> (a -> String) -> (String -> ReaderT MyTardisConfig IO (Result a)) -> ReaderT MyTardisConfig IO (Result a)
updateResource resource getURI getFn = do
    apiBase <- myTardisApiBase <$> ask

    r <- putWith' (dropIfPrefix apiBase $ getURI resource) (toJSON resource)

    let url = B8.unpack <$> r ^? responseHeader "Location"

    let status = r ^. responseStatus . statusCode

    host <- myTardisHost <$> ask

    if status `elem` [200, 204]
        then getFn $ getURI resource
        else return $ Error $ "Failed to update resource: " ++ show status

data LookupError = NoMatches | TwoOrMoreMatches | OtherError String deriving (Eq, Show)

-- | Retrieve a resource. Returns 'Right' if exactly one match is found, otherwise 'Left' and some error information.
getResourceWithMetadata :: forall a b. (Show a, Show b, Eq b)
    => ReaderT MyTardisConfig IO (Result [a])
    -> (a -> ReaderT MyTardisConfig IO b)               -- ^ Function that retrieves the full list of resources, e.g. 'getExperiments'.
    -> b                                                -- ^ Function that converts a rest value (e.g. 'RestExperiment') to an identified value (e.g. 'IdentifiedExperiment').
    -> ReaderT MyTardisConfig IO (Either LookupError a) -- ^ A uniquely discovered resource, or a lookup error.
getResourceWithMetadata getFn restToIdentified identifiedObject = do
    objects  <- getFn                                    :: ReaderT MyTardisConfig IO (Result [a])
    objects' <- traverse (mapM restToIdentified) objects :: ReaderT MyTardisConfig IO (Result [b])

    return $ case filter ((==) identifiedObject . snd) <$> (liftM2 zip) objects objects' of
        Success [singleMatch] -> Right $ fst singleMatch
        Success []            -> Left NoMatches
        Success _             -> Left TwoOrMoreMatches
        Error e               -> Left $ OtherError e

-- | Given an identified experiment, find a matching experiment in MyTARDIS.
getExperimentWithMetadata :: IdentifiedExperiment -> ReaderT MyTardisConfig IO (Either LookupError RestExperiment)
getExperimentWithMetadata = getResourceWithMetadata getExperiments restExperimentToIdentified

-- | Given an identified dataset, find a matching dataset in MyTARDIS.
getDatasetWithMetadata :: IdentifiedDataset -> ReaderT MyTardisConfig IO (Either LookupError RestDataset)
getDatasetWithMetadata = getResourceWithMetadata getDatasets restDatasetToIdentified

-- | Given an identified file, find a matching file in MyTARDIS.
getFileWithMetadata :: IdentifiedFile -> ReaderT MyTardisConfig IO (Either LookupError RestDatasetFile)
getFileWithMetadata = getResourceWithMetadata getDatasetFiles restFileToIdentified

-- | Create an experiment. If an experiment already exists in MyTARDIS that matches our
-- locally identified experiment, we just return the existing experiment.
createExperiment :: IdentifiedExperiment -> ReaderT MyTardisConfig IO (Result RestExperiment)
createExperiment ie@(IdentifiedExperiment description instutitionName title metadata) = do
    x <- getExperimentWithMetadata ie

    case x of
        Right re        -> return $ Success re
        Left NoMatches  -> createResource "/experiment/" getExperiment m
        Left  _         -> return $ Error "Duplicate experiment detected, will not create another."
  where
    m = object
            [ ("description",       String $ T.pack description)
            , ("institution_name",  String $ T.pack instutitionName)
            , ("title",             String $ T.pack title)
            , ("parameter_sets",    toJSON $ map metadataToJSON metadata)
            , ("objectacls",        toJSON $ ([] :: [RestGroup]))
            ]

createUser :: Maybe String -> Maybe String -> String -> [RestGroup] -> Bool -> ReaderT MyTardisConfig IO (Result RestUser)
createUser firstname lastname username groups isSuperuser = createResource "/user/" getUser m
  where
    m = object
            [ ("first_name",    String $ T.pack $ fromMaybe "" firstname)
            , ("groups",        toJSON groups)
            , ("last_name",     String $ T.pack $ fromMaybe "" lastname)
            , ("username",      String $ T.pack username)
            , ("is_superuser",  toJSON isSuperuser)
            ]

metadataToJSON :: (String, M.Map String String) -> Value
metadataToJSON (name, mmap) = mkParameterSet name (M.toList mmap)

-- | Create a dataset. If a dataset already exists in MyTARDIS that matches our
-- locally identified dataset, we just return the existing dataset.
createDataset :: IdentifiedDataset -> ReaderT MyTardisConfig IO (Result RestDataset)
createDataset ids@(IdentifiedDataset description experiments metadata) = do
    x <- getDatasetWithMetadata ids

    case x of
        Right rds       -> return $ Success rds
        Left NoMatches  -> createResource "/dataset/" getDataset m
        Left  _         -> return $ Error "Duplicate dataset detected, will not create another."

  where
    m = object
            [ ("description",       String $ T.pack description)
            , ("experiments",       toJSON experiments)
            , ("immutable",         toJSON True)
            , ("parameter_sets",    toJSON $ map metadataToJSON metadata)
            ]

createGroup :: String -> ReaderT MyTardisConfig IO (Result RestGroup)
createGroup name = createResource "/group/" getGroup x
  where
    x = object
            [ ("name",          String $ T.pack name)
            , ("permissions",   toJSON ([] :: [RestPermission]))
            ]

createExperimentObjectACL :: Bool -> Bool -> Bool -> RestGroup -> RestExperiment -> ReaderT MyTardisConfig IO (Result RestObjectACL)
createExperimentObjectACL canDelete canRead canWrite group experiment = createResource "/objectacl/" getObjectACL x
  where
    x = object
            [ ("aclOwnershipType",  toJSON (1 :: Integer))
            , ("content_type",      toJSON ("experiment" :: String))
            , ("canDelete",         toJSON canDelete)
            , ("canRead",           toJSON canRead)
            , ("canWrite",          toJSON canWrite)
            , ("entityId",          String $ T.pack $ show $ groupID group)
            , ("isOwner",             toJSON False)
            , ("object_id",           toJSON $ eiID experiment)
            , ("pluginId",            toJSON ("django_group" :: String))
            ]

-- | For a local file, calculate its canonical file path, md5sum, and size.
calcFileMetadata :: FilePath -> IO (Maybe (String, String, Integer))
calcFileMetadata _filepath = do
    filepath <- canonicalizePath _filepath
    md5sum   <- computeChecksum filepath
    size     <- toInteger . fileSize <$> getFileStatus filepath

    return $ case md5sum of
        Right md5sum' -> Just (filepath, md5sum', size)
        Left _        -> Nothing

-- | Create a location for a dataset file in MyTARDIS.
createFileLocation :: IdentifiedFile -> ReaderT MyTardisConfig IO (Result RestDatasetFile)
createFileLocation idf@(IdentifiedFile datasetURL filepath md5sum size metadata) = do
    -- Match on just the base bit of the file, not the whole path.
    let filepath' = takeFileName filepath

    x <- getFileWithMetadata $ idf {idfFilePath = filepath'}

    case x of
        Right dsf -> return $ Success dsf
        Left NoMatches      -> let m = object
                                        [ ("dataset",           String $ T.pack datasetURL)
                                        , ("filename",          String $ T.pack $ takeFileName filepath)
                                        , ("md5sum",            String $ T.pack md5sum)
                                        , ("size",              toJSON $ size)
                                        , ("parameter_sets",    toJSON $ map metadataToJSON metadata)
                                        ] in createResource "/dataset_file/" getDatasetFile m

        Left _              -> return $ Error "Duplicate file detected, will not create another."

-- | Generic function for creating a resource.
getResource :: forall a. FromJSON a => String -> ReaderT MyTardisConfig IO (Result a)
getResource uri = do
    MyTardisConfig host _ _ _ opts <- ask
    r <- liftIO $ getWith opts (host ++ uri)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getParameterName :: String -> ReaderT MyTardisConfig IO (Result RestParameterName)
getParameterName = getResource

getExperimentParameterSet :: String -> ReaderT MyTardisConfig IO (Result RestExperimentParameterSet)
getExperimentParameterSet = getResource

getExperimentParameter :: String -> ReaderT MyTardisConfig IO (Result RestParameter)
getExperimentParameter = getResource

getExperiment :: String -> ReaderT MyTardisConfig IO (Result RestExperiment)
getExperiment = getResource

getSchema :: String -> ReaderT MyTardisConfig IO (Result RestSchema)
getSchema = getResource

getPermission :: String -> ReaderT MyTardisConfig IO (Result RestPermission)
getPermission = getResource

getGroup :: String -> ReaderT MyTardisConfig IO (Result RestGroup)
getGroup = getResource

getObjectACL :: String -> ReaderT MyTardisConfig IO (Result RestObjectACL)
getObjectACL = getResource

getUser :: String -> ReaderT MyTardisConfig IO (Result RestUser)
getUser = getResource

data MyTardisSchema = SchemaExperiment | SchemaDataset | SchemaDatasetFile | SchemaNone

-- These could be instances of something so that the types
-- are constructed properly.

readSchema :: (Eq a, Num a, Show a) => a -> MyTardisSchema
readSchema 1 = SchemaExperiment
readSchema 2 = SchemaDataset
readSchema 3 = SchemaDatasetFile
readSchema 4 = SchemaNone
readSchema x = error $ "No MyTARDIS schema associated with " ++ show x

showSchema ::  MyTardisSchema -> Integer
showSchema SchemaExperiment     = 1
showSchema SchemaDataset        = 2
showSchema SchemaDatasetFile    = 3
showSchema SchemaNone           = 4

createSchema :: String -> String -> MyTardisSchema -> ReaderT MyTardisConfig IO (Result RestSchema)
createSchema name namespace stype = do
    createResource "/schema/" getSchema m
  where
    m = object
            [ ("name",              String $ T.pack name)
            , ("namespace",         String $ T.pack namespace)
            , ("type",              Number $ fromIntegral $ showSchema stype)
            ]

getDataset :: String -> ReaderT MyTardisConfig IO (Result RestDataset)
getDataset = getResource

getDatasetFile :: String -> ReaderT MyTardisConfig IO (Result RestDatasetFile)
getDatasetFile = getResource

getTopLevel :: T.Text -> BL.ByteString -> Maybe Value
getTopLevel name b = join $ parseObjects <$> join (decode b)
  where
    parseObjects :: FromJSON a => Object -> Maybe a
    parseObjects = parseMaybe (.: name)

getObjects :: BL.ByteString -> Maybe Value
getObjects = getTopLevel "objects"

getMeta :: BL.ByteString -> Maybe Value
getMeta = getTopLevel "meta"

getList :: forall a. FromJSON a => String -> ReaderT MyTardisConfig IO (Result [a])
getList url = do
    MyTardisConfig host apiBase _ _ opts <- ask
    body <- liftIO $ (^? responseBody) <$> getWith opts (host ++ apiBase ++ url)

    let objects = join $ getObjects <$> body

        next   = case (fmap emNext) <$> (fromJSON <$> (join $ getMeta <$> body)) of
                    (Just (Success (Just nextURL))) -> Just $ dropIfPrefix apiBase nextURL
                    _                               -> Nothing

    let this = maybeToResult $ fromJSON <$> objects :: Result [a]

    case next of Just next' -> do rest <- getList next'
                                  return $ (liftM2 (++)) this rest
                 Nothing    -> return this

  where

    -- This is almost asum from Data.Foldable?
    maybeToResult :: Maybe (Result t) -> Result t
    maybeToResult (Just x) = x
    maybeToResult Nothing  = Error "Nothing"

dropIfPrefix :: String -> String -> String
dropIfPrefix prefix s = if prefix `isPrefixOf` s
                            then drop (length prefix) s
                            else s


getExperimentParameterSets :: ReaderT MyTardisConfig IO (Result [RestExperimentParameterSet])
getExperimentParameterSets = getList "/experimentparameterset"

getExperimentParameters :: ReaderT MyTardisConfig IO (Result [RestParameter])
getExperimentParameters = getList "/experimentparameter"

getExperiments :: ReaderT MyTardisConfig IO (Result [RestExperiment])
getExperiments = getList "/experiment"

getDatasets :: ReaderT MyTardisConfig IO (Result [RestDataset])
getDatasets = getList "/dataset"

getDatasetFiles :: ReaderT MyTardisConfig IO (Result [RestDatasetFile])
getDatasetFiles = getList "/dataset_file"

getReplicas :: ReaderT MyTardisConfig IO (Result [RestReplica])
getReplicas = getList "/replica"

getSchemas :: ReaderT MyTardisConfig IO (Result [RestSchema])
getSchemas = getList "/schema"

getPermissions :: ReaderT MyTardisConfig IO (Result [RestPermission])
getPermissions = getList "/permission"

getParameterNames :: ReaderT MyTardisConfig IO (Result [RestParameterName])
getParameterNames = getList "/parametername"

getGroups :: ReaderT MyTardisConfig IO (Result [RestGroup])
getGroups = getList "/group"

getUsers :: ReaderT MyTardisConfig IO (Result [RestUser])
getUsers = getList "/user"

getObjectACLs :: ReaderT MyTardisConfig IO (Result [RestObjectACL])
getObjectACLs = getList "/objectacl"

getUnverifiedReplicas :: ReaderT MyTardisConfig IO (Result [RestReplica])
getUnverifiedReplicas = fmap (filter (not . replicaVerified)) <$> getReplicas

mkParameter :: (String, String) -> Value
mkParameter (name, value) = object [ ("name",  String $ T.pack name)
                                   , ("value", String $ T.pack value)
                                   ]

mkParameterSet :: String -> [(String, String)] -> Value
mkParameterSet schema nameValuePairs = object
    [ ("schema", String $ T.pack schema)
    , ("parameters", toJSON $ map mkParameter nameValuePairs)
    ]

copyFileToStore :: FilePath -> RestDatasetFile -> IO ()
copyFileToStore filepath dsf = forM_ (dsfReplicas dsf) $ \r -> do
    let filePrefix = "file://" :: String
        target = replicaURL r

    when (not (replicaVerified r) && isPrefixOf filePrefix target) $ do
        let targetFilePath = drop (length filePrefix) target
        print (filepath, targetFilePath)
        copyFile filepath targetFilePath

deleteExperiment :: RestExperiment -> ReaderT MyTardisConfig IO (Response ())
deleteExperiment x = do
    MyTardisConfig host apiBase _ _ opts <- ask
    liftIO $ deleteWith opts $ host ++ eiResourceURI x

deleteDataset :: RestDataset -> ReaderT MyTardisConfig IO (Response ())
deleteDataset x = do
    MyTardisConfig host apiBase _ _ opts <- ask
    liftIO $ deleteWith opts $ host ++ dsiResourceURI x

deleteDatasetFile :: RestDatasetFile -> ReaderT MyTardisConfig IO (Response ())
deleteDatasetFile x = do
    MyTardisConfig host apiBase _ _ opts <- ask
    liftIO $ deleteWith opts $ host ++ dsfResourceURI x

restExperimentToIdentified :: RestExperiment -> ReaderT MyTardisConfig IO IdentifiedExperiment
restExperimentToIdentified rexpr = do
    metadata <- mapM handyParameterSet (eiParameterSets rexpr)

    return $ IdentifiedExperiment
                (eiDescription rexpr)
                (eiInstitutionName rexpr)
                (eiTitle rexpr)
                metadata

restDatasetToIdentified :: RestDataset -> ReaderT MyTardisConfig IO IdentifiedDataset
restDatasetToIdentified rds = do
    metadata <- mapM handyParameterSet (dsiParameterSets rds)

    return $ IdentifiedDataset
                (dsiDescription rds)
                (dsiExperiments rds)
                metadata

restFileToIdentified :: RestDatasetFile -> ReaderT MyTardisConfig IO IdentifiedFile
restFileToIdentified rf = do
    metadata <- mapM handyParameterSet (dsfParameterSets rf)

    return $ IdentifiedFile
                (dsfDatasetURL  rf)
                (dsfFilename    rf)
                (dsfMd5sum      rf)
                (read $ dsfSize rf)
                metadata

class GeneralParameterSet a where
    generalGetParameters :: a -> [RestParameter]
    generalGetSchema     :: a -> RestSchema

instance GeneralParameterSet RestExperimentParameterSet where
    generalGetParameters = epsParameters
    generalGetSchema     = epsSchema

instance GeneralParameterSet RestDatasetParameterSet where
    generalGetParameters = dsParamSetParameters
    generalGetSchema     = dsParamSetSchema

instance GeneralParameterSet RestDatasetFileParameterSet where
    generalGetParameters = dsfParamSetParameters
    generalGetSchema     = dsfParamSetSchema

-- | Get a parameterset in a handy (schema, dict) form.
handyParameterSet :: GeneralParameterSet a => a -> ReaderT MyTardisConfig IO (String, M.Map String String)
handyParameterSet paramset = do
    m <- (M.fromList . catMaybes) <$> mapM getBoth (generalGetParameters paramset)
    return (schema, m)

  where

    schema = schemaNamespace $ generalGetSchema paramset

    getParName :: RestParameter -> ReaderT MyTardisConfig IO (Result String)
    getParName eparam = fmap pnName <$> getParameterName (epName eparam)

    getBoth :: RestParameter -> ReaderT MyTardisConfig IO (Maybe (String, String))
    getBoth eparam = do
        name <- getParName eparam
        let value = epStringValue eparam

        case (name, value) of (Success name', Just value') -> return $ Just (name', value')
                              _                            -> return Nothing

-- | Get a Group with a given name, or create it if it doesn't already exist. Will fail if
-- a duplicate group name is discovered.
getOrCreateGroup  :: String -> ReaderT MyTardisConfig IO (Result RestGroup)
getOrCreateGroup name = do
    groups <- traverse (filter (((==) name) . groupName)) <$> getGroups

    case groups of
        []              -> createGroup name
        [Success group] -> return $ Success group
        _               -> return $ Error $ "Duplicate groups found with name: " ++ name

-- | Create an ObjectACL that gives a group read-only access to an experiment.
getOrCreateExperimentACL  :: Bool -> Bool -> Bool -> RestExperiment -> RestGroup -> ReaderT MyTardisConfig IO (Result RestObjectACL)
getOrCreateExperimentACL canDelete canRead canWrite experiment group = do
    acls <- traverse (filter f) <$> getObjectACLs

    case acls of
        []            -> createExperimentObjectACL canDelete canRead canWrite group experiment
        [Success acl] -> return $ Success acl
        _             -> return $ Error $ "Duplicate Object ACLs found for: " ++ show experiment ++ " " ++ show group

  where

    f acl =  objectAclOwnershipType acl == 1
          && objectCanDelete        acl == canDelete
          && objectCanRead          acl == canRead
          && objectCanWrite         acl == canWrite
          && objectEntityId         acl == show (groupID group)
          && objectIsOwner          acl == False
          && objectObjectID         acl == eiID experiment
          && objectPluginId         acl == "django_group"

addUserToGroup  :: RestUser -> RestGroup -> ReaderT MyTardisConfig IO (Result RestUser)
addUserToGroup user group = do
    if any ((==) (groupName group)) (map groupName currentGroups)
        then return $ Success user
        else updateResource (user { ruserGroups = group:currentGroups }) ruserResourceURI getUser
  where
    currentGroups = ruserGroups user

removeUserFromGroup  :: RestUser -> RestGroup -> ReaderT MyTardisConfig IO (Result RestUser)
removeUserFromGroup user group = updateResource user' ruserResourceURI getUser
  where
    user' = user
              { ruserGroups = filter ((/=) group) (ruserGroups user)
              }

-- | Allow a Group to have read-only access to an experiment.
addGroupReadOnlyAccess  :: RestExperiment -> RestGroup -> ReaderT MyTardisConfig IO (Result RestExperiment)
addGroupReadOnlyAccess = addGroupAccess False True False

addGroupAccess  :: Bool -> Bool -> Bool -> RestExperiment -> RestGroup -> ReaderT MyTardisConfig IO (Result RestExperiment)
addGroupAccess canDelete canRead canWrite experiment group = do
    acl <- getOrCreateExperimentACL canDelete canRead canWrite experiment group

    -- Note: we don't need to update the experiment, we just have
    -- to create the ObjectACL referring to the Experiment's ID.
 
    case acl of
        Success acl' -> getExperiment (eiResourceURI experiment)
        Error   e    -> return $ Error e

  where

    -- Avoid adding the ACL if we have one with the same general attributes.
    addAcl  :: RestObjectACL -> [RestObjectACL] -> [RestObjectACL]
    addAcl acl acls = if any (aclEq acl) acls
                            then acls
                            else acl:acls

    aclEq :: RestObjectACL -> RestObjectACL -> Bool
    aclEq acl1 acl2 =  eqOn objectAclOwnershipType acl1 acl2
                    && eqOn objectCanDelete        acl1 acl2
                    && eqOn objectCanRead          acl1 acl2
                    && eqOn objectCanWrite         acl1 acl2
                    && eqOn objectEntityId         acl1 acl2
                    && eqOn objectIsOwner          acl1 acl2
                    && eqOn objectObjectID         acl1 acl2
                    && eqOn objectPluginId         acl1 acl2

    eqOn :: Eq b => (a -> b) -> a -> a -> Bool
    eqOn f x y = (f x) == (f y)
