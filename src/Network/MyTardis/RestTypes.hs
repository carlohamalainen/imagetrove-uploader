{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}

module Network.MyTardis.RestTypes where

import Prelude hiding (id)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (mzero)
import Data.Aeson
import Data.Typeable

data RestExperimentMeta = RestExperimentMeta
    { emLimit :: Integer
    , emNext  :: Maybe String
    , emOffset :: Integer
    , emPrevious :: Maybe String
    , emTotalCount :: Integer
    } deriving (Eq, Show)

instance FromJSON RestExperimentMeta where
    parseJSON (Object v) = RestExperimentMeta <$>
        v .: "limit" <*>
        v .: "next" <*>
        v .: "offset" <*>
        v .: "previous" <*>
        v .: "total_count"
    parseJSON _          = mzero

-- From tardis/tardis_portal/models/experiment.py:
publicAccessNone, publicAccessMetadata, publicAccessFull :: Integer
publicAccessNone        = 1
publicAccessMetadata    = 50
publicAccessFull        = 100

data RestExperiment = RestExperiment
    { eiApproved        :: Bool
    , eiCreatedBy       :: String
    , eiCreatedTime     :: String
    , eiDescription     :: String       -- ^ Experiment description.
    , eiEndTime         :: Maybe String
    , eiHandle          :: Maybe String
    , eiID              :: Integer
    , eiInstitutionName :: String
    , eiParameterSets   :: [RestExperimentParameterSet] -- ^ Experiment parameter sets.
    , eiLocked          :: Bool
    , eiPublicAccess    :: Integer  -- ^ Access level; see PUBLIC_ACCESS_NONE, PUBLIC_ACCESS_METADATA, and PUBLIC_ACCESS_FULL in the MyTARDIS source.
    , eiResourceURI     :: String   -- ^ Experiment resource URI, e.g. \"\/api\/v1\/experiment\/42\/\"
    , eiStartTime       :: Maybe String
    , eiTitle           :: String
    , eiUpdateTime      :: String
    , eiURL             :: Maybe String
    , eiObjectACLs      :: [RestObjectACL]
    }
    deriving (Eq, Ord, Show, Typeable)

instance FromJSON RestExperiment where
    parseJSON (Object v) = RestExperiment <$>
        v .: "approved"         <*>
        v .: "created_by"       <*>
        v .: "created_time"     <*>
        v .: "description"      <*>
        v .: "end_time"         <*>
        v .: "handle"           <*>
        v .: "id"               <*>
        v .: "institution_name" <*>
        v .: "parameter_sets"   <*>
        v .: "locked"           <*>
        v .: "public_access"    <*>
        v .: "resource_uri"     <*>
        v .: "start_time"       <*>
        v .: "title"            <*>
        v .: "update_time"      <*>
        v .: "url"              <*>
        v .: "objectacls"
    parseJSON _          = mzero

instance ToJSON RestExperiment where
    toJSON e  = object
                    [ ("approved",            toJSON $ eiApproved e)
                    , ("created_by",          toJSON $ eiCreatedBy e)
                    , ("created_time",        toJSON $ eiCreatedTime e)
                    , ("description",         toJSON $ eiDescription e)
                    , ("end_time",            toJSON $ eiEndTime e)
                    , ("handle",              toJSON $ eiHandle e)
                    , ("id",                  toJSON $ eiID e)
                    , ("institution_name",    toJSON $ eiInstitutionName e)
                    , ("parameter_sets",      toJSON $ eiParameterSets e)
                    , ("locked",              toJSON $ eiLocked e)
                    , ("public_access",       toJSON $ eiPublicAccess e)
                    , ("resource_uri",        toJSON $ eiResourceURI e)
                    , ("start_time",          toJSON $ eiStartTime e)
                    , ("title",               toJSON $ eiTitle e)
                    , ("update_time",         toJSON $ eiUpdateTime e)
                    , ("url",                 toJSON $ eiURL e)
                    , ("objectacls",          toJSON $ eiObjectACLs e)
                    ]

data RestParameter = RestParameter
    { epDatetimeValue   :: Maybe String
    , epID              :: Integer
    , epName            :: String       -- ^ URI to parametername, e.g. \"\/api\/v1\/parametername\/63\/\"
    , epNumericalValue  :: Maybe Float  -- ^ Numerical value.
    , epParametersetURL :: String       -- ^ URI to parameterset, e.g. \"\/api\/v1\/experimentparameterset\/13\/\"
    , epResourceURI     :: String       -- ^ URI for this parameter, e.g. \"\/api\/v1\/experimentparameter\/33\/\"
    , epStringValue     :: Maybe String -- ^ String value.
    , epValue           :: Maybe String
    } deriving (Eq, Ord, Show)

instance FromJSON RestParameter where
    parseJSON (Object v) = RestParameter <$>
        v .: "datetime_value"   <*>
        v .: "id"               <*>
        v .: "name"             <*>
        v .: "numerical_value"  <*>
        v .: "parameterset"     <*>
        v .: "resource_uri"     <*>
        v .: "string_value"     <*>
        v .: "value"
    parseJSON _          = mzero

instance ToJSON RestParameter where
    toJSON (RestParameter datetime id name numval pset uri sval _) = object
                                                                            [ ("datetime_value",    toJSON datetime)
                                                                            , ("id",                toJSON id)
                                                                            , ("name",              toJSON name)
                                                                            , ("numerical_value",   toJSON numval)
                                                                            , ("parameterset",      toJSON pset)
                                                                            , ("resource_uri",      toJSON uri)
                                                                            , ("string_value",      toJSON sval)
                                                                            ]

data RestExperimentParameterSet = RestExperimentParameterSet
    { epsExperimentURL      :: String           -- ^ URI for the experiment that this parameter set refers to.
    , epsID                 :: Integer
    , epsParameters         :: [RestParameter]  -- ^ List of parameters that are in this parameter set.
    , epsResourceURI        :: String           -- ^ Resource URI for this parameter set.
    , epsSchema             :: RestSchema       -- ^ Schema for this parameter set.
    } deriving (Eq, Ord, Show)

instance FromJSON RestExperimentParameterSet where
    parseJSON (Object v) = RestExperimentParameterSet <$>
        v .: "experiment"   <*>
        v .: "id"           <*>
        v .: "parameters"   <*>
        v .: "resource_uri" <*>
        v .: "schema"
    parseJSON _          = mzero

instance ToJSON RestExperimentParameterSet where
    toJSON (RestExperimentParameterSet e id ps uri schema) = object
                                                                [ ("experiment",   toJSON e)
                                                                , ("id",           toJSON id)
                                                                , ("parameters",   toJSON ps)
                                                                , ("resource_uri", toJSON uri)
                                                                , ("schema",       toJSON schema)
                                                                ]

data RestDatasetParameterSet = RestDatasetParameterSet
    { dsParamSetDatasetURL     :: String
    , dsParamSetID             :: Integer
    , dsParamSetParameters     :: [RestParameter]
    , dsParamSetResourceURI    :: String
    , dsParamSetSchema         :: RestSchema
    }
    deriving (Eq, Show)

instance FromJSON RestDatasetParameterSet where
    parseJSON (Object v) = RestDatasetParameterSet <$>
        v .: "dataset"   <*>
        v .: "id"           <*>
        v .: "parameters"   <*>
        v .: "resource_uri" <*>
        v .: "schema"
    parseJSON _          = mzero

data RestDatasetFileParameterSet = RestDatasetFileParameterSet
    { dsfParamSetFileURL        :: String
    , dsfParamSetID             :: Integer
    , dsfParamSetParameters     :: [RestParameter]
    , dsfParamSetResourceURI    :: String
    , dsfParamSetSchema         :: RestSchema
    }
    deriving (Eq, Show)

instance FromJSON RestDatasetFileParameterSet where
    parseJSON (Object v) = RestDatasetFileParameterSet <$>
        v .: "dataset_file"     <*>
        v .: "id"               <*>
        v .: "parameters"       <*>
        v .: "resource_uri"     <*>
        v .: "schema"
    parseJSON _          = mzero

data RestSchema = RestSchema
    { schemaHidden      :: Bool
    , schemaID          :: Integer
    , schemaImmutable   :: Bool
    , schemaName        :: String           -- ^ Name, e.g. \"DICOM fields\".
    , schemaNamespace   :: String           -- ^ Namespace (primary key), e.g. \"http:\/\/cai.uq.edu.au\/schema\/metadata\/1\"
    , schemaResourceURI :: String           -- ^ Resource URI for this schema.
    , schemaSubtype     :: Maybe String
    , schemaType        :: Integer          -- ^ Schema types, hard coded in the MyTARDIS source: EXPERIMENT = 1, DATASET = 2, DATAFILE = 3, NONE = 4.
    } deriving (Eq, Ord, Show)

instance FromJSON RestSchema where
    parseJSON (Object v) = RestSchema <$>
        v .: "hidden"       <*>
        v .: "id"           <*>
        v .: "immutable"    <*>
        v .: "name"         <*>
        v .: "namespace"    <*>
        v .: "resource_uri" <*>
        v .: "subtype"      <*>
        v .: "type"
    parseJSON _          = mzero

instance ToJSON RestSchema where
    toJSON (RestSchema hidden id immutable name namespace uri subtype t) = object
                                                                            [ ("hidden",          toJSON hidden)
                                                                            , ("id",              toJSON id)
                                                                            , ("immutable",       toJSON immutable)
                                                                            , ("name",            toJSON name)
                                                                            , ("namespace",       toJSON namespace)
                                                                            , ("resource_uri",    toJSON uri)
                                                                            , ("subtype",         toJSON subtype)
                                                                            , ("type",            toJSON t)
                                                                            ]

data RestParameterName = RestParameterName
    { pnChoices             :: Maybe String
    , pnComparisonType      :: Integer
    , pnDataType            :: Integer
    , pnFullName            :: String
    , pnID                  :: Integer
    , pnImmutable           :: Bool
    , pnIsSearchable        :: Bool
    , pnName                :: String
    , pnOrder               :: Integer
    , pnResourceURI         :: String
    , pnSchemaURL           :: String
    , pnUnits               :: Maybe String
    } deriving (Eq, Ord, Show)

instance FromJSON RestParameterName where
    parseJSON (Object v) = RestParameterName <$>
        v .: "choices"          <*>
        v .: "comparison_type"  <*>
        v .: "data_type"        <*>
        v .: "full_name"        <*>
        v .: "id"               <*>
        v .: "immutable"        <*>
        v .: "is_searchable"    <*>
        v .: "name"             <*>
        v .: "order"            <*>
        v .: "resource_uri"     <*>
        v .: "schema"           <*>
        v .: "units"
    parseJSON _          = mzero

-- The hell does the 'dsi' prefix mean?
data RestDataset = RestDataset
    { dsiDescription     :: String
    , dsiDirectory       :: Maybe String
    , dsiExperiments     :: [String]
    , dsiID              :: Integer
    , dsiImmutable       :: Bool
    , dsiParameterSets   :: [RestDatasetParameterSet]
    , dsiResourceURI     :: String
    } deriving (Eq, Show)

instance FromJSON RestDataset where
    parseJSON (Object v) = RestDataset <$>
        v .: "description"  <*>
        v .: "directory"    <*>
        v .: "experiments"  <*>
        v .: "id"           <*>
        v .: "immutable"    <*>
        v .: "parameter_sets" <*>
        v .: "resource_uri"
    parseJSON _          = mzero

data RestDatasetFile = RestDatasetFile
    { dsfCreatedTime      :: Maybe String
    , dsfDatafile         :: Maybe String
    , dsfDatasetURL       :: String         -- ^ URI for dataset that has this file.
    , dsfDirectory        :: Maybe String   -- ^ Directory name (used in the tar file that users can download).
    , dsfFilename         :: String         -- ^ File name. Not the whole path.
    , dsfID               :: Integer
    , dsfMd5sum           :: String         -- ^ Md5sum.
    , dsfMimetype         :: String         -- ^ Mimetype.
    , dsfModificationTime :: Maybe String
    , dsfParameterSets    :: [RestDatasetFileParameterSet] -- ^ List of parameter sets attached to this file.
    , dsfReplicas         :: [RestReplica]                  -- ^ Replicas. Typically these point to a file in the MyTARDIS store directory but could in future be object store etc.
    , dsfResourceURI      :: String         -- ^ Resource URI for this file.
    , dsfSha512sum        :: String
    , dsfSize             :: String         -- ^ File size. Yes, it is a string!
    } deriving (Eq, Show)

instance FromJSON RestDatasetFile where
    parseJSON (Object v) = RestDatasetFile <$>
        v .: "created_time"         <*>
        v .: "datafile"             <*>
        v .: "dataset"              <*>
        v .: "directory"            <*>
        v .: "filename"             <*>
        v .: "id"                   <*>
        v .: "md5sum"               <*>
        v .: "mimetype"             <*>
        v .: "modification_time"    <*>
        v .: "parameter_sets"    <*>
        v .: "replicas"             <*>
        v .: "resource_uri"         <*>
        v .: "sha512sum"            <*>
        v .: "size"
    parseJSON _          = mzero

data RestReplica = RestReplica
    { replicaDatafile           :: String       -- ^ URI for the file.
    , replicaID                 :: Integer
    , replicaLocation           :: String       -- ^ URI for the location where this file is stored (see the Location model in MyTARDIS).
    , replicaProtocol           :: Maybe String
    , replicaResourceURI        :: String       -- ^ URI for this replica.
    , replicaStayRemote         :: Bool
    , replicaURL                :: String       -- ^ URL to this file, not including location, e.g. \"11\/11\/foo.mnc\",
    , replicaVerified           :: Bool         -- ^ Has MyTARDIS verified the file's checksum?
    } deriving (Eq, Show)

instance FromJSON RestReplica where
    parseJSON (Object v) = RestReplica <$>
        v .: "datafile"         <*>
        v .: "id"               <*>
        v .: "location"         <*>
        v .: "protocol"         <*>
        v .: "resource_uri"     <*>
        v .: "stay_remote"      <*>
        v .: "url"              <*>
        v .: "verified"
    parseJSON _          = mzero

data RestPermission = RestPermission
    { permCodename    :: String
    , permID          :: Integer
    , permName        :: String
    , permResourceURI :: String
    } deriving (Eq, Ord, Show)

instance FromJSON RestPermission where
    parseJSON (Object v) = RestPermission <$>
        v .: "codename"         <*>
        v .: "id"               <*>
        v .: "name"             <*>
        v .: "resource_uri"
    parseJSON _          = mzero

instance ToJSON RestPermission where
    toJSON (RestPermission codename id name uri) = object
                                            [ ("codename",      toJSON codename)
                                            , ("id",            toJSON id)
                                            , ("name",          toJSON name)
                                            , ("resource_uri",  toJSON uri)
                                            ]

data RestGroup = RestGroup
    { groupID             :: Integer
    , groupName           :: String
    , groupPermissions    :: [RestPermission]
    , groupResourceURI    :: String
    } deriving (Eq, Ord, Show)

instance FromJSON RestGroup where
    parseJSON (Object v) = RestGroup <$>
        v .: "id"               <*>
        v .: "name"             <*>
        v .: "permissions"      <*>
        v .: "resource_uri"
    parseJSON _          = mzero

instance ToJSON RestGroup where
    toJSON (RestGroup id name perms uri) = object
                                            [ ("id",            toJSON id)
                                            , ("name",          toJSON name)
                                            , ("permissions",   toJSON perms)
                                            , ("resource_uri",  toJSON uri)
                                            ]
data RestObjectACL = RestObjectACL
    { objectAclOwnershipType  :: Integer
    , objectCanDelete   :: Bool
    , objectCanRead     :: Bool
    , objectCanWrite    :: Bool
    , objectEffectiveDate   :: Maybe String
    , objectEntityId    :: String -- ^ ID of object that we are referring to, e.g. "RestExperiment".
    , objectExpiryDate  :: Maybe String
    , objectID          :: Integer
    , objectIsOwner     :: Bool
    , objectObjectID    :: Integer
    , objectPluginId    :: String -- ^ For example \"django_group\".
    , objectRelatedGroup   :: Maybe RestGroup
    , objectResourceURI     :: String
    } deriving (Eq, Ord, Show)

instance FromJSON RestObjectACL where
    parseJSON (Object v) = RestObjectACL <$>
        v .: "aclOwnershipType"     <*>
        v .: "canDelete"            <*>
        v .: "canRead"              <*>
        v .: "canWrite"             <*>
        v .: "effectiveDate"        <*>
        v .: "entityId"             <*>
        v .: "expiryDate"           <*>
        v .: "id"                   <*>
        v .: "isOwner"              <*>
        v .: "object_id"            <*>
        v .: "pluginId"             <*>
        v .: "related_group"        <*>
        v .: "resource_uri"
    parseJSON _          = mzero

instance ToJSON RestObjectACL where
    toJSON (RestObjectACL ownertype candel canread canwrite effdate eid edate id isowner oid pluginid _ _) = object
        [ ("aclOwnershipType",  toJSON ownertype)
        , ("canDelete",         toJSON candel)
        , ("canRead",           toJSON canread)
        , ("canWrite",          toJSON canwrite)
        , ("effectiveDate",     toJSON effdate)
        , ("entityId",          toJSON eid)
        , ("expiryDate",        toJSON edate)
        , ("id",                toJSON id)
        , ("isOwner",           toJSON isowner)
        , ("object_id",         toJSON oid)
        , ("pluginId",          toJSON pluginid)
        ]

data RestUser = RestUser
    { ruserFirstName    :: String
    , ruserGroups       :: [RestGroup]
    , ruserLastName     :: String
    , ruserResourceURI  :: String
    , ruserUsername     :: String
    , ruserIsSuperuser  :: Bool
    } deriving (Eq, Show)

instance FromJSON RestUser where
    parseJSON (Object v) = RestUser <$>
        v .: "first_name"           <*>
        v .: "groups"               <*>
        v .: "last_name"            <*>
        v .: "resource_uri"         <*>
        v .: "username"             <*>
        v .: "is_superuser"
    parseJSON _          = mzero

instance ToJSON RestUser where
    toJSON (RestUser fname groups lname _ username issuper) = object
        [ ("first_name",    toJSON fname)
        , ("groups",        toJSON groups)
        , ("last_name",     toJSON lname)
        , ("username",      toJSON username)
        , ("is_superuser",  toJSON issuper)
        ]
