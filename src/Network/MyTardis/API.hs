{-# LANGUAGE OverloadedStrings, RankNTypes, ScopedTypeVariables #-}

module Network.MyTardis.API where

import Control.Applicative ((<$>))
import Control.Lens
import Control.Monad (join, forM_, when, liftM)

import Control.Monad.Reader
import Control.Monad.Identity
import Control.Monad.Trans (liftIO)

import Data.Aeson (Result(..))
import Data.Either
import Data.Maybe
import Control.Applicative
import Control.Exception.Base (catch, IOException(..))
import Control.Lens
import Control.Monad
import Control.Monad.Identity
import Control.Monad.Reader
import Data.Maybe
import Network.Wreq
import Safe
import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.Map as M

import Data.Aeson
import Data.Aeson.Types
import Data.List (isPrefixOf)
import Data.Maybe
import Data.Traversable (traverse)
import Network.HTTP.Client (defaultManagerSettings, managerResponseTimeout)
import Network.Mime
import Network.Wreq

import Safe

import Network.MyTardis.Instances

import System.Directory
import System.FilePath
import System.Posix.Files

import qualified Data.ByteString.Lazy   as BL
import qualified Data.ByteString.Char8  as B8

import qualified Data.Text as T
import Text.Printf (printf)
import qualified Data.Map as M

import Data.Dicom
import Network.ImageTrove.Utils (computeChecksum)
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

-- | Wrapper around Wreq's 'putWith' that uses our settings in a 'ReaderT'.
putWith' :: String -> Value -> ReaderT MyTardisConfig IO (Response BL.ByteString)
putWith' x v = do
    MyTardisConfig host apiBase user pass opts <- ask
    liftIO $ putWith opts (host ++ apiBase ++ x) v

-- | Create a resource. See 'createExperiment' for an example use.
createResource
    :: String                                           -- ^ Name of resource used to construct the full URL, e.g. \"/experiment/\"
    -> (String -> ReaderT MyTardisConfig IO (Result a)) -- ^ Function to retrieve the newly created resource.
    -> Value                                            -- ^ Metadata, JSON form.
    -> ReaderT MyTardisConfig IO (Result a)             -- ^ The new resource.
createResource resourceURL getFn keyValueMap = do
    r <- postWith' resourceURL (toJSON keyValueMap)

    let url = B8.unpack <$> r ^? responseHeader "Location"

    host <- myTardisHost <$> ask

    case url of Just url' -> getFn $ drop (length host) url'
                Nothing   -> return $ Error "Failed to fetch newly created resource."

-- | Update a resource.
updateResource :: ToJSON a =>
       a                                                    -- ^ Resource to update, e.g. 'RestUser'.
    -> (a -> String)                                        -- ^ Function to get URI from the resource.
    -> (String -> ReaderT MyTardisConfig IO (Result a))     -- ^ Function to retrieve the updated resource.
    -> ReaderT MyTardisConfig IO (Result a)                 -- ^ The updated resource.
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
    => ReaderT MyTardisConfig IO (Result [a])   -- ^ Function that retrieves the full list of resources, e.g. 'getExperiments'.
    -> (a -> ReaderT MyTardisConfig IO b)       -- ^ Convert a resource to its "identified" equivalent, e.g. 'IdentifiedExperiment'.
    -> b                                        -- ^ The identified value, e.g. 'IdentifiedExperiment'.
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
            , ("parameter_sets",    toJSON $ map mkParameterSet metadata)
            , ("objectacls",        toJSON $ ([] :: [RestGroup]))
            ]

-- | Create a new user in MyTARDIS' Django backend.
createUser
    :: Maybe String                                 -- ^ User's first name.
    -> Maybe String                                 -- ^ User's last name.
    -> String                                       -- ^ Username.
    -> [RestGroup]                                  -- ^ Groups that this user is a member of.
    -> Bool                                         -- ^ Create a Django superuser.
    -> ReaderT MyTardisConfig IO (Result RestUser)  -- ^ The new user resource.
createUser firstname lastname username groups isSuperuser = createResource "/user/" getUser m
  where
    m = object
            [ ("first_name",    String $ T.pack $ fromMaybe "" firstname)
            , ("groups",        toJSON groups)
            , ("last_name",     String $ T.pack $ fromMaybe "" lastname)
            , ("username",      String $ T.pack username)
            , ("is_superuser",  toJSON isSuperuser)
            ]

-- | Construct a ParameterSet from a basic "String"/"String" map.
mkParameterSet :: (String, M.Map String String) -- ^ Schema URL and key/value map.
               -> Value                         -- ^ JSON value.
mkParameterSet (schemaURL, mmap) = object
                                        [ ("schema",     String $ T.pack schemaURL)
                                        , ("parameters", toJSON $ map mkParameter (M.toList mmap))
                                        ]
  where
    mkParameter :: (String, String) -> Value
    mkParameter (name, value) = object
                                    [ ("name",  String $ T.pack name)
                                    , ("value", String $ T.pack value)
                                    ]



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
            , ("parameter_sets",    toJSON $ map mkParameterSet metadata)
            ]

-- | Create a Django group. We do not set any permissions
-- here, e.g.  add_logentry, change_logentry, delete_logentry, etc.
createGroup
    :: String                                       -- ^ Group name, e.g. \"Project 12345\".
    -> ReaderT MyTardisConfig IO (Result RestGroup) -- ^ The newly created group.
createGroup name = createResource "/group/" getGroup x
  where
    x = object
            [ ("name",          String $ T.pack name)
            , ("permissions",   toJSON ([] :: [RestPermission]))
            ]

-- | Create an ObjectACL for an experiment.
createExperimentObjectACL
    :: Bool                                             -- ^ canDelete
    -> Bool                                             -- ^ canRead
    -> Bool                                             -- ^ canWrite
    -> RestGroup                                        -- ^ Group for which the ACL refers to.
    -> RestExperiment                                   -- ^ Experiment for which the ACL refers to.
    -> ReaderT MyTardisConfig IO (Result RestObjectACL) -- ^ The new ObjectACL resource.
createExperimentObjectACL canDelete canRead canWrite group experiment = createResource "/objectacl/" getObjectACL x
  where
    x = object
            [ ("aclOwnershipType",  toJSON (1 :: Integer))
            , ("content_type",      toJSON ("experiment" :: String))
            , ("canDelete",         toJSON canDelete)
            , ("canRead",           toJSON canRead)
            , ("canWrite",          toJSON canWrite)
            , ("entityId",          String $ T.pack $ show $ groupID group)
            , ("isOwner",           toJSON False)
            , ("object_id",         toJSON $ eiID experiment)
            , ("pluginId",          toJSON ("django_group" :: String))
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

-- | Create a location for a dataset file in MyTARDIS. Fails if a matching file exists in this dataset.
-- It is our responsibility to copy the supplied location. See "copyFileToStore". Until we do so (and the checksums match)
-- the related "RestReplica" will have @replicaVerified == False@.
createFileLocation
    :: IdentifiedFile                                     -- ^ Identified file.
    -> ReaderT MyTardisConfig IO (Result RestDatasetFile) -- New dataset file resource.
createFileLocation idf@(IdentifiedFile datasetURL filepath md5sum size metadata) = do
    -- Match on just the base bit of the file, not the whole path.
    let filepath' = takeFileName filepath

    -- FIXME This gets a list of *ALL* dataset files, which could be huge, not
    -- the list of files in the dataset referred to by datasetURL!
    x <- getFileWithMetadata $ idf {idfFilePath = filepath'}

    case x of
        Right dsf -> return $ Error "Matching file exists. We will not create another."
        Left NoMatches        -> let m = object
                                        [ ("dataset",           String $ T.pack datasetURL)
                                        , ("filename",          String $ T.pack $ takeFileName filepath)
                                        , ("md5sum",            String $ T.pack md5sum)
                                        , ("size",              toJSON $ size)
                                        , ("parameter_sets",    toJSON $ map mkParameterSet metadata)
                                        ] in createResource "/dataset_file/" getDatasetFile m

        Left TwoOrMoreMatches -> return $ Error $ "Found two or more matches for this file, will not create another: " ++ filepath

        Left (OtherError e)   -> return $ Error e

type URI = String

-- | Generic function for creating a resource.
getResource :: forall a. FromJSON a => URI -> ReaderT MyTardisConfig IO (Result a)
getResource uri = do
    MyTardisConfig host _ _ _ opts <- ask
    r <- liftIO $ getWith opts (host ++ uri)

    return $ case (join $ decode <$> r ^? responseBody :: Maybe Value) of
        Just v      -> fromJSON v
        Nothing     -> Error "Could not decode resource."

getParameterName :: URI -> ReaderT MyTardisConfig IO (Result RestParameterName)
getParameterName = getResource

getExperimentParameterSet :: URI -> ReaderT MyTardisConfig IO (Result RestExperimentParameterSet)
getExperimentParameterSet = getResource

getExperimentParameter :: URI -> ReaderT MyTardisConfig IO (Result RestParameter)
getExperimentParameter = getResource

getExperiment :: URI -> ReaderT MyTardisConfig IO (Result RestExperiment)
getExperiment = getResource

getSchema :: URI -> ReaderT MyTardisConfig IO (Result RestSchema)
getSchema = getResource

getPermission :: URI -> ReaderT MyTardisConfig IO (Result RestPermission)
getPermission = getResource

getGroup :: URI -> ReaderT MyTardisConfig IO (Result RestGroup)
getGroup = getResource

getObjectACL :: URI -> ReaderT MyTardisConfig IO (Result RestObjectACL)
getObjectACL = getResource

getUser :: URI -> ReaderT MyTardisConfig IO (Result RestUser)
getUser = getResource

data MyTardisSchema = SchemaExperiment | SchemaDataset | SchemaDatasetFile | SchemaNone

-- | Create an experiment, dataset, or file schema. The @namespace@ is the primary key in Django, not the @name@.
createSchema
    :: String                                           -- ^ Name, e.g. @DICOM Metadata Experiment@.
    -> String                                           -- ^ Namespace. Must be a url form, e.g. @http://cai.edu.au/schema/1@.
    -> MyTardisSchema                                   -- ^ Schema type.
    -> ReaderT MyTardisConfig IO (Result RestSchema)    -- ^ New schema resource.
createSchema name namespace stype = do
    createResource "/schema/" getSchema m
  where
    m = object
            [ ("name",              String $ T.pack name)
            , ("namespace",         String $ T.pack namespace)
            , ("type",              Number $ fromIntegral $ showSchema stype)
            ]

    showSchema ::  MyTardisSchema -> Integer
    showSchema SchemaExperiment     = 1
    showSchema SchemaDataset        = 2
    showSchema SchemaDatasetFile    = 3
    showSchema SchemaNone           = 4

-- | Retrieve a dataset.
getDataset :: URI -> ReaderT MyTardisConfig IO (Result RestDataset)
getDataset = getResource

-- | Retrieve a dataset file.
getDatasetFile :: URI -> ReaderT MyTardisConfig IO (Result RestDatasetFile)
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

-- | Retrieve all experiment parameter sets.
getExperimentParameterSets :: ReaderT MyTardisConfig IO (Result [RestExperimentParameterSet])
getExperimentParameterSets = getList "/experimentparameterset"

-- | Retrieve all experiment parameters.
getExperimentParameters :: ReaderT MyTardisConfig IO (Result [RestParameter])
getExperimentParameters = getList "/experimentparameter"

-- | Retrieve all experiments.
getExperiments :: ReaderT MyTardisConfig IO (Result [RestExperiment])
getExperiments = getList "/experiment"

-- | Retrieve all datasets.
getDatasets :: ReaderT MyTardisConfig IO (Result [RestDataset])
getDatasets = getList "/dataset"

-- | Retrieve all dataset files.
getDatasetFiles :: ReaderT MyTardisConfig IO (Result [RestDatasetFile])
getDatasetFiles = getList "/dataset_file"

-- | Retrieve all replicas.
getReplicas :: ReaderT MyTardisConfig IO (Result [RestReplica])
getReplicas = getList "/replica"

-- | Retrieve all schemas.
getSchemas :: ReaderT MyTardisConfig IO (Result [RestSchema])
getSchemas = getList "/schema"

-- | Retrieve all permissions.
getPermissions :: ReaderT MyTardisConfig IO (Result [RestPermission])
getPermissions = getList "/permission"

-- | Retrieve all parameter names.
getParameterNames :: ReaderT MyTardisConfig IO (Result [RestParameterName])
getParameterNames = getList "/parametername"

-- | Retrieve all groups.
getGroups :: ReaderT MyTardisConfig IO (Result [RestGroup])
getGroups = getList "/group"

-- | Retrieve all users.
getUsers :: ReaderT MyTardisConfig IO (Result [RestUser])
getUsers = getList "/user"

-- | Retrieve all ObjectACLs.
getObjectACLs :: ReaderT MyTardisConfig IO (Result [RestObjectACL])
getObjectACLs = getList "/objectacl"

-- | Retrieve all replicas that are not verified.
getUnverifiedReplicas :: ReaderT MyTardisConfig IO (Result [RestReplica])
getUnverifiedReplicas = fmap (filter $ not . replicaVerified) <$> getReplicas

-- | Copy a file to the MyTARDIS store. Currently this only supports local files - nothing about web locations
-- (as in the Atom Provider) or an object store. The return value is a list of @Result FilePath@ because a dataset file
-- in MyTARDIS has a list of replicas. Typically we only use one, but for completeness we have to check all of them.
copyFileToStore
    :: FilePath                 -- ^ Path of a local file.
    -> RestDatasetFile          -- ^ Dataset file (should be unverified) with a valid location.
    -> IO ([Result FilePath])   -- ^ List of results of attempting to copy @filepath@ to the location in the dataset file resource.
copyFileToStore filepath dsf = do
    results <- forM (dsfReplicas dsf) $ \r -> do
        let filePrefix = "file://" :: String
            target = replicaURL r

        if (not $ isPrefixOf filePrefix target)
            then return $ Error $ "Unknown prefix: " ++ target
            else
                if (not $ replicaVerified r)
                    then do let targetFilePath = drop (length filePrefix) target
                            catch (copyFile filepath targetFilePath >> return (Success targetFilePath))
                                  (\e -> return $ Error $ show (e :: IOException))
                    else return $ Success filepath

    return results

-- | Delete an experiment.
deleteExperiment :: RestExperiment -> ReaderT MyTardisConfig IO (Response ())
deleteExperiment x = do
    MyTardisConfig host apiBase _ _ opts <- ask
    liftIO $ deleteWith opts $ host ++ eiResourceURI x

-- | Delete a dataset.
deleteDataset :: RestDataset -> ReaderT MyTardisConfig IO (Response ())
deleteDataset x = do
    MyTardisConfig host apiBase _ _ opts <- ask
    liftIO $ deleteWith opts $ host ++ dsiResourceURI x

-- | Delete a dataset file.
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
handyParameterSet :: GeneralParameterSet a
    => a  -- An instance of "GeneralParameterSet".
    -> ReaderT MyTardisConfig IO (String, M.Map String String) -- Schema namespace and key/value map.
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

-- Get a user if they already exist (matching on the @username@) otherwise create the user.
-- If a user already exists and they have @isSuperuser == False@ and we call this function
-- with @isSuperuser == True@, we will not change them to a superuser. Similarly, calling
-- this function with changing values for @groups@ will not update the user's group membership.
getOrCreateUser
    :: Maybe String     -- ^ User's first name.
    -> Maybe String     -- ^ User's last name.
    -> String           -- ^ Username.
    -> [RestGroup]      -- ^ Rest groups that the user should be a member of.
    -> Bool             -- ^ Create as a superuser.
    -> ReaderT MyTardisConfig IO (Result RestUser)
getOrCreateUser firstname lastname username groups isSuperuser = do
    users <- traverse (filter (((==) username) . ruserUsername)) <$> getUsers

    case users of
        []              -> createUser firstname lastname username groups isSuperuser 
        [Success user]  -> return $ Success user
        _               -> return $ Error $ "Duplicate users found with username: " ++ username

-- | Get a group with a given name, or create it if it doesn't already exist. Will fail if
-- a duplicate group name is discovered.
getOrCreateGroup  :: String -> ReaderT MyTardisConfig IO (Result RestGroup)
getOrCreateGroup name = do
    groups <- traverse (filter $ ((==) name) . groupName) <$> getGroups

    case groups of
        []              -> createGroup name
        [Success group] -> return $ Success group
        _               -> return $ Error $ "Duplicate groups found with name: " ++ name

-- | Get or create an ObjectACL that gives a group specified access to an experiment.
getOrCreateExperimentACL
    :: Bool                 -- ^ canDelete
    -> Bool                 -- ^ canRead
    -> Bool                 -- ^ canWrite
    -> RestExperiment       -- ^ Experiment.
    -> RestGroup            -- ^ Group.
    -> ReaderT MyTardisConfig IO (Result RestObjectACL) -- ^ Updated ObjectACL.
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

-- | Make a user a member of a group.
addUserToGroup  :: RestUser -> RestGroup -> ReaderT MyTardisConfig IO (Result RestUser)
addUserToGroup user group = do
    if any ((==) (groupName group)) (map groupName currentGroups)
        then return $ Success user
        else updateResource (user { ruserGroups = group:currentGroups }) ruserResourceURI getUser
  where
    currentGroups = ruserGroups user

-- | Remove a group from a user's membership list.
removeUserFromGroup  :: RestUser -> RestGroup -> ReaderT MyTardisConfig IO (Result RestUser)
removeUserFromGroup user group = updateResource user' ruserResourceURI getUser
  where
    user' = user { ruserGroups = filter ((/=) group) (ruserGroups user) }

-- | Allow a group to have read-only access to an experiment.
addGroupReadOnlyAccess  :: RestExperiment -> RestGroup -> ReaderT MyTardisConfig IO (Result RestExperiment)
addGroupReadOnlyAccess = addGroupAccess False True False

-- | Add the specified group access to an experiment.
addGroupAccess
    :: Bool                                                 -- ^ canDelete
    -> Bool                                                 -- ^ canRead
    -> Bool                                                 -- ^ canWrite
    -> RestExperiment                                       -- ^ Experiment.
    -> RestGroup                                            -- ^ Group.
    -> ReaderT MyTardisConfig IO (Result RestExperiment)    -- ^ Updated experment resource.
addGroupAccess canDelete canRead canWrite experiment group = do
    acl <- getOrCreateExperimentACL canDelete canRead canWrite experiment group

    -- Note: we don't need to update the experiment, we just have
    -- to create the ObjectACL referring to the Experiment's ID.

    case acl of
        Success acl' -> getExperiment (eiResourceURI experiment)
        Error   e    -> return $ Error e

uploadFileBasic
    :: String
    -> (RestDataset -> String -> String -> Integer -> [(String, M.Map String String)] -> IdentifiedFile)
    -> RestDataset
    -> FilePath
    -> [(String, M.Map String String)]
    -> ReaderT MyTardisConfig IO (Result RestDatasetFile)
uploadFileBasic schemaFile identifyDatasetFile d f m = do
    meta <- liftIO $ calcFileMetadata f

    -- This would be tidier if we worked in the Success monad?
    case meta of
        Just  (filepath, md5sum, size) -> do let idf = identifyDatasetFile d filepath md5sum size m
                                             dsf <- createFileLocation idf

                                             case dsf of Success dsf' -> do results <- liftIO $ copyFileToStore f dsf'
                                                                            return $ case allSuccesses results of
                                                                                Success _ -> Success dsf'
                                                                                Error e   -> Error e
                                                         Error e      -> return $ Error e
        Nothing                        -> return $ Error $ "Failed to calculate checksum for " ++ f

allSuccesses :: [Result a] -> Result String
allSuccesses []               = Success "All Success or empty list."
allSuccesses ((Success _):xs) = allSuccesses xs
allSuccesses ((Error e):_)    = Error e


createSchemasIfMissing :: (String, String, String) -> ReaderT MyTardisConfig IO (Result (RestSchema, RestSchema, RestSchema))
createSchemasIfMissing (schemaExperiment, schemaDataset, schemaFile) = do
    schemas <- getSchemas

    case schemas of
        Error e -> return $ Error e
        Success schemas' -> do experimentSchema <- createIfMissing "DICOM Metadata Experiment" schemaExperiment SchemaExperiment  schemas'
                               datasetSchema    <- createIfMissing "DICOM Metadata Dataset"    schemaDataset    SchemaDataset     schemas'
                               fileSchema       <- createIfMissing "DICOM Metadata File"       schemaFile       SchemaDatasetFile schemas'

                               return $ case (experimentSchema, datasetSchema, fileSchema) of
                                         (Success experimentSchema', Success datasetSchema', Success fileSchema') -> Success (experimentSchema', datasetSchema', fileSchema')
                                         _                                                                        -> Error "Failed to create schema (which one?)."

  where

    schemaExists  :: String -> [RestSchema] -> Bool
    schemaExists ns schemas = any ((==) ns . schemaNamespace) schemas

    -- createIfMissing :: String -> [RestSchema] -> ???
    createIfMissing name namespace stype schemas = if schemaExists namespace schemas
                                    then return $ Success $ head $ filter ((==) namespace . schemaNamespace) schemas
                                    else createSchema name namespace stype

-- | Convert the DICOM files in a directory to MINC and upload.
uploadDicomAsMinc
  :: [DicomFile -> Bool]                    -- ^ Filters to match instrument.
     -> [DicomFile -> Maybe String]         -- ^ Fields to use in the experiment's title.
     -> [DicomFile -> Maybe String]         -- ^ Fields to use in the dataset's title.
     -> (String
         -> String
         -> String
         -> String
         -> [DicomFile -> Maybe String]
         -> [DicomFile]
         -> Maybe IdentifiedExperiment)     -- ^ Identify an experiment.
     -> (String
         -> [DicomFile -> Maybe String]
         -> RestExperiment
         -> [DicomFile]
         -> Maybe IdentifiedDataset)        -- ^ Identify a dataset.
     -> (RestDataset
         -> String
         -> String
         -> Integer
         -> [(String, M.Map String String)]
         -> IdentifiedFile)                 -- ^ Identify a file.
     -> FilePath                            -- ^ Directory containing DICOM files.
     -> (String, String, String, String, String, String) -- ^ Experiment schema, dataset schema, file schema, default institution name, default department name, default institution address.
     -> ReaderT MyTardisConfig IO ()

uploadDicomAsMinc instrumentFilters experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile dir (schemaExperiment, schemaDataset, schemaFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress) = do
    _files1 <- liftIO $ rights <$> (getDicomFilesInDirectory ".dcm" dir >>= mapM readDicomMetadata)
    _files2 <- liftIO $ rights <$> (getDicomFilesInDirectory ".IMA" dir >>= mapM readDicomMetadata)
    let _files = _files1 ++ _files2
    liftIO $ putStrLn $ "uploadDicomAsMinc: |_files| = " ++ (show $ length _files)

    let groups = groupDicomFiles instrumentFilters experimentFields datasetFields _files
    liftIO $ putStrLn $ "uploadDicomAsMinc: |groups| = " ++ (show $ length groups)

    forM_ groups $ \files -> uploadDicomAsMincOneGroup files instrumentFilters experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile dir (schemaExperiment, schemaDataset, schemaFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress)

uploadDicomAsMincOneGroup files instrumentFilters experimentFields datasetFields identifyExperiment identifyDataset identifyDatasetFile dir (schemaExperiment, schemaDataset, schemaFile, defaultInstitutionName, defaultInstitutionalDepartmentName, defaultInstitutionalAddress) = do

    schemas <- createSchemasIfMissing (schemaExperiment, schemaDataset, schemaFile)
 
    -- FIXME Just doing some defaults at the moment, dangerously
    -- assuming Success at each step.
    Success users <- getUsers
    let admin = head $ filter ((==) "admin" . ruserUsername) users -- FIXME assumes account exists...
    Success adminGroup <- getOrCreateGroup "admin"
    addUserToGroup admin adminGroup
    liftIO $ putStrLn $ "uploadDicomAsMinc: set admin group."

    let Just ie@(IdentifiedExperiment desc institution title metadata) = identifyExperiment
                                                                            schemaExperiment
                                                                            defaultInstitutionName
                                                                            defaultInstitutionalDepartmentName
                                                                            defaultInstitutionalAddress
                                                                            experimentFields
                                                                            files

    Success e <- createExperiment ie -- FIXME pattern
    liftIO $ putStrLn $ "uploadDicomAsMinc: " ++ show e
    addGroupReadOnlyAccess e adminGroup
 
    -- FIXME Just pattern...
    let Just ids@(IdentifiedDataset desc experiments metadata) = identifyDataset schemaDataset datasetFields e files

    Success d <- createDataset ids -- FIXME
    liftIO $ putStrLn $ "uploadDicomAsMinc: " ++ show d

    let oneFile = head files -- FIXME unsafe
        filemetadata = [(schemaFile, M.fromList
                                            [ ("PatientID",          fromMaybe "(PatientID missing)"   $ dicomPatientID         oneFile)
                                            , ("StudyInstanceUID",   fromMaybe "(StudyInstanceUID missing)"    $ dicomStudyInstanceUID  oneFile)
                                            , ("SeriesInstanceUID",  fromMaybe "(SeriesInstanceUID missing)" $ dicomSeriesInstanceUID oneFile)
                                            , ("StudyDescription",  fromMaybe "(StudyDescription missing)" $ dicomStudyDescription oneFile)
                                            ]
                        )
                       ]

    -- Now pack the dicom files as Minc
    dicom <- liftIO $ dicomToMinc $ map dicomFilePath files
    case dicom of
        Right mincFiles -> do -- Convert to MINC 2.0
                              liftIO $ forM_ mincFiles mncToMnc2 -- FIXME check results

                              forM_ mincFiles $ \f -> do
                                    liftIO $ putStrLn $ "uploadDicomAsMinc: minc file f: " ++ show f
                                    dsf <- uploadFileBasic schemaFile identifyDatasetFile d f filemetadata
                                    liftIO $ putStrLn $ "uploadDicomAsMinc: dsf: " ++ show dsf

                                    thumbnail <- liftIO $ createMincThumbnail f
                                    liftIO $ putStrLn $ "uploadDicomAsMinc: thumbnail: " ++ show thumbnail

                                    case thumbnail of
                                        Right thumbnail' -> do dsft <- uploadFileBasic schemaFile identifyDatasetFile d thumbnail' filemetadata
                                                               liftIO $ putStrLn $ "uploadDicomAsMinc: dsft: " ++ show dsft
                                        Left e           -> liftIO $ putStrLn $ "Error while creating thumbnail: " ++ e ++ " for file " ++ f

    return (e, d)
