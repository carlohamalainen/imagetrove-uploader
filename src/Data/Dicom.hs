{-# LANGUAGE DeriveDataTypeable, OverloadedStrings, RankNTypes #-}

module Data.Dicom where

import Prelude hiding (FilePath)

import Data.Either
import Data.Char (toLower)
import Data.List hiding (find)
import Data.Maybe
import Control.Applicative ((<$>), (<*>), (<*))
import Control.Monad
import Control.Monad.Catch
import Control.Monad.Identity
import Control.Monad.Reader
import Control.Monad.ST
import Control.Monad.Trans.Writer.Lazy ()
import Data.Function (on)
import Data.Ord (comparing)
import Data.Functor.Identity
import Data.List (isInfixOf, groupBy, isSuffixOf)
import Data.Typeable
import System.Directory
import System.FilePath
import System.FilePath.Glob
import Text.HTML.TagSoup.Entity (lookupEntity)
import Text.Parsec
import Control.Exception.Base (catch, IOException(..))

import System.IO.Temp
import System.Posix.Files
import System.Process
import System.Exit (ExitCode(..))

import System.Unix.Directory (removeRecursiveSafely)

import System.FilePath.Find

import Network.ImageTrove.Utils (runShellCommand)

-- http://stackoverflow.com/a/7233657/3659845
unescapeEntities :: String -> String
unescapeEntities [] = []
unescapeEntities ('&':xs) =
  let (b, a) = break (== ';') xs in
  case (lookupEntity b, a) of
    (Just c, ';':as) ->  c ++ unescapeEntities as
    _                -> '&' : unescapeEntities xs
unescapeEntities (x:xs) = x : unescapeEntities xs

getRecursiveContentsList :: FilePath -> IO [FilePath]
getRecursiveContentsList path = find always (fileType ==? RegularFile) path

dcmDump :: FilePath -> IO (Either String String)
dcmDump f = runShellCommand (dropFileName f) "dcmdump" ["+Qn", "+L", "-M", f]

globDcmFiles :: FilePath -> IO [FilePath]
globDcmFiles path = do
    -- FIXME does namesMatching do a case insensitive match?
    dcm <- namesMatching $ path </> "*.dcm"
    ima <- namesMatching $ path </> "*.IMA"

    return $ dcm ++ ima

createLinksDirectory :: FilePath -> FilePath -> IO FilePath
createLinksDirectory tmp dicomDir = globDcmFiles dicomDir >>= createLinksDirectoryFromList tmp

createLinksDirectoryFromList :: FilePath -> [FilePath] -> IO FilePath
createLinksDirectoryFromList tmp dicomFiles = do
    tempDir <- createTempDirectory tmp "dicomConversion"

    print dicomFiles

    forM_ dicomFiles $ \f -> do
        sourceName <- canonicalizePath f
        let target = tempDir </> (takeFileName f ++ ".dcm")

        -- print $ ("createLinksDirectoryFromList", sourceName, target)
        createSymbolicLink sourceName target

    return tempDir

dicomToMinc :: FilePath -> [FilePath] -> IO (Either String (FilePath, [FilePath]))
dicomToMinc tmp dicomFiles = do
    dicomDir' <- createLinksDirectoryFromList tmp dicomFiles
    outputDir <- createTempDirectory tmp "dcm2mnc"

    putStrLn $ "dicomToMinc: " ++ show ("dcm2mnc", [dicomDir', outputDir])

    result <- runShellCommand tmp "dcm2mnc" [dicomDir', outputDir] -- FIXME OK to run from tmp?

    putStrLn $ "dicomToMinc: result: " ++ show result


    removeRecursiveSafely dicomDir'

    case result of Right result' -> (Right . (,) outputDir) <$> getRecursiveContentsList outputDir
                   Left e        -> return $ Left e

mncToMnc2 :: FilePath -> FilePath -> IO (Either String FilePath)
mncToMnc2 tmp filePath = do
    tmpFile <- fst <$> openTempFile tmp "mincto2.mnc"

    result <- runShellCommand (dropFileName filePath) "mincconvert" ["-2", "-clobber", filePath, tmpFile]

    case result of Right _ -> do renameFile tmpFile filePath
                                 return $ Right filePath
                   Left e  -> return $ Left e

createMincThumbnail :: FilePath -> IO (Either String FilePath)
createMincThumbnail mncFile = do
    let mincThumbnail = mncFile ++ ".png"

    result <- runShellCommand (dropFileName mncFile) "mincpik" [mncFile, mincThumbnail]

    case result of Right _ -> return $ Right mincThumbnail
                   Left e  -> return $ Left e

createNifti :: FilePath -> IO (Either String FilePath)
createNifti mncFile = do
    let base = reverse . (drop 3) . reverse $ mncFile
        niftiFile = base ++ "nii"

    result <- runShellCommand (dropFileName mncFile) "mnc2nii" [mncFile, niftiFile]

    case result of Right _ -> return $ Right niftiFile
                   Left e  -> return $ Left e

parseField :: forall u. String -> ParsecT String u Identity String
parseField prefix = do
    _ <- string prefix

    _ <- string " ["

    field <- many (noneOf "]")

    _ <- char ']'

    _ <- many anyChar

    return field


pPatientName                = parseField "(0010,0010) PN"
pPatientID                  = parseField "(0010,0020) LO"
pPatientBirthDate           = parseField "(0010,0030) DA"
pPatientSex                 = parseField "(0010,0040) CS"
pPatientAge                 = parseField "(0010,1010) AS"
pPatientWeight              = parseField "(0010,1030) DS"
pPatientPosition            = parseField "(0018,5100) CS"

pStudyDate                  = parseField "(0008,0020) DA"
pStudyTime                  = parseField "(0008,0030) TM"
pStudyDescription           = parseField "(0008,1030) LO"
pStudyInstanceID            = parseField "(0020,000d) UI"
pStudyID                    = parseField "(0020,0010) SH"

pSeriesDate                 = parseField "(0008,0021) DA"
pSeriesTime                 = parseField "(0008,0031) TM"
pSeriesDescription          = parseField "(0008,103e) LO"
pSeriesInstanceUID          = parseField "(0020,000e) UI"
pSeriesNumber               = parseField "(0020,0011) IS"
pCSASeriesHeaderType        = parseField "(0029,1018) CS"
pCSASeriesHeaderVersion     = parseField "(0029,1019) LO"
pCSASeriesHeaderInfo        = parseField "(0029,1020) OB"
pSeriesWorkflowStatus       = parseField "(0029,1160) LO"

pMediaStorageSOPInstanceUID = parseField "(0002,0003) UI"
pInstanceCreationDate       = parseField "(0008,0012) DA"
pInstanceCreationTime       = parseField "(0008,0013) TM"
pSOPInstanceUID             = parseField "(0008,0018) UI"
pStudyInstanceUID           = parseField "(0020,000d) UI"
pInstanceNumber             = parseField "(0020,0013) IS"

pInstitutionName             = parseField "(0008,0080) LO"
pInstitutionAddress          = parseField "(0008,0081) ST"
pInstitutionalDepartmentName = parseField "(0008,1040) LO"

pReferringPhysicianName      = parseField "(0008,0090) PN"

pManufacturerModelName       = parseField "(0008,1090) LO"
pManufacturer                = parseField "(0008,0070) LO"

pSequenceName                = parseField "(0018,0024) SH"

pStationName                 = parseField "(0008,1010) SH"

parseSingleMatch :: ParsecT String () Identity String -> String -> Maybe String
parseSingleMatch p s = case parses of
                            ["(no value available)"] -> Nothing
                            [match]                  -> Just match
                            _                        -> Nothing
  where
    parses = rights $ map (parse p "(parseSingleMatch)") (lines s)


fieldToFn "PatientName"         = dicomPatientName
fieldToFn "PatientID"           = dicomPatientID
fieldToFn "PatientBirthDate"    = dicomPatientBirthDate
fieldToFn "PatientSex"          = dicomPatientSex
fieldToFn "PatientAge"          = dicomPatientAge
fieldToFn "PatientWeight"       = dicomPatientWeight
fieldToFn "PatientPosition"     = dicomPatientPosition

fieldToFn "StudyDate"           = dicomStudyDate
fieldToFn "StudyTime"           = dicomStudyTime
fieldToFn "StudyDescription"    = dicomStudyDescription
fieldToFn "StudyInstanceID"     = dicomStudyInstanceID
fieldToFn "StudyID"             = dicomStudyID

fieldToFn "SeriesDate"              = dicomSeriesDate
fieldToFn "SeriesTime"              = dicomSeriesTime
fieldToFn "SeriesDescription"       = dicomSeriesDescription
fieldToFn "SeriesInstanceUID"       = dicomSeriesInstanceUID
fieldToFn "SeriesNumber"            = dicomSeriesNumber
fieldToFn "CSASeriesHeaderType"     = dicomCSASeriesHeaderType
fieldToFn "CSASeriesHeaderVersion"  = dicomCSASeriesHeaderVersion
fieldToFn "CSASeriesHeaderInfo"     = dicomCSASeriesHeaderInfo
fieldToFn "SeriesWorkflowStatus"    = dicomSeriesWorkflowStatus

fieldToFn "MediaStorageSOPInstanceUID"  = dicomMediaStorageSOPInstanceUID
fieldToFn "InstanceCreationDate"        = dicomInstanceCreationDate
fieldToFn "InstanceCreationTime"        = dicomInstanceCreationTime
fieldToFn "SOPInstanceUID"              = dicomSOPInstanceUID
fieldToFn "StudyInstanceUID"            = dicomStudyInstanceUID
fieldToFn "InstanceNumber"              = dicomInstanceNumber

fieldToFn "InstitutionName"             = dicomInstitutionName
fieldToFn "InstitutionAddress"          = dicomInstitutionAddress
fieldToFn "InstitutionalDepartmentName" = dicomInstitutionalDepartmentName

fieldToFn "ReferringPhysicianName"  = dicomReferringPhysicianName

fieldToFn "ManufacturerModelName"   = dicomManufacturerModelName
fieldToFn "Manufacturer"            = dicomManufacturer

fieldToFn "SequenceName"            = dicomSequenceName

fieldToFn "StationName"             = dicomStationName

fieldToFn f = error $ "Unknown DICOM field name [" ++ f ++ "]. Please report this."

tupleToIdentifierFn :: (String, String) -> DicomFile -> Bool
tupleToIdentifierFn (field, value) d = fn d == Just value
  where
    fn = fieldToFn field

data DicomFile = DicomFile
    { dicomFilePath                   :: FilePath
    , dicomPatientName                :: Maybe String
    , dicomPatientID                  :: Maybe String
    , dicomPatientBirthDate           :: Maybe String
    , dicomPatientSex                 :: Maybe String
    , dicomPatientAge                 :: Maybe String
    , dicomPatientWeight              :: Maybe String
    , dicomPatientPosition            :: Maybe String

    , dicomStudyDate                  :: Maybe String
    , dicomStudyTime                  :: Maybe String
    , dicomStudyDescription           :: Maybe String
    , dicomStudyInstanceID            :: Maybe String
    , dicomStudyID                    :: Maybe String

    , dicomSeriesDate                 :: Maybe String
    , dicomSeriesTime                 :: Maybe String
    , dicomSeriesDescription          :: Maybe String
    , dicomSeriesInstanceUID          :: Maybe String
    , dicomSeriesNumber               :: Maybe String
    , dicomCSASeriesHeaderType        :: Maybe String
    , dicomCSASeriesHeaderVersion     :: Maybe String
    , dicomCSASeriesHeaderInfo        :: Maybe String
    , dicomSeriesWorkflowStatus       :: Maybe String

    , dicomMediaStorageSOPInstanceUID :: Maybe String
    , dicomInstanceCreationDate       :: Maybe String
    , dicomInstanceCreationTime       :: Maybe String
    , dicomSOPInstanceUID             :: Maybe String
    , dicomStudyInstanceUID           :: Maybe String
    , dicomInstanceNumber             :: Maybe String


    , dicomInstitutionName              :: Maybe String
    , dicomInstitutionAddress           :: Maybe String
    , dicomInstitutionalDepartmentName  :: Maybe String

    , dicomReferringPhysicianName       :: Maybe String

    , dicomManufacturerModelName        :: Maybe String
    , dicomManufacturer                 :: Maybe String

    , dicomSequenceName                 :: Maybe String

    , dicomStationName                  :: Maybe String
    }
    deriving (Eq, Show)

data DicomException = DicomMissingSeriesInstanceUID FilePath
                    | ShellError String
                    | DicomMissingSeriesDescription FilePath
    deriving (Show, Typeable)

instance Exception DicomException

readSeriesDesc :: FilePath -> IO String
readSeriesDesc f = do
{-
    x <- runShellCommand (dropFileName f) "dcmdump" ["-s", "+P", "0020,000e", f]

    case x of
        Left e      -> throwM $ ShellError e
        Right x'    -> case (unescapeEntities <$> parseSingleMatch pSeriesInstanceUID x') of
                            Nothing   -> throwM $ DicomMissingSeriesInstanceUID f
                            Just suid -> return suid
-}
    seriesDesc <- runShellCommand (dropFileName f) "dcmdump" ["-s", "+P", "0008,103e", f]
    -- seriesNr   <- runShellCommand (dropFileName f) "dcmdump" ["-s", "+P", "0020,0011", f]

    {-
    case (seriesDesc, seriesNr) of
        (Right seriesDesc', Right seriesNr') -> do let seriesDesc'' = unescapeEntities <$> parseSingleMatch pSeriesDescription seriesDesc'
                                                       seriesNr''   = unescapeEntities <$> parseSingleMatch pSeriesNumber      seriesNr'
                                                   case (seriesDesc'', seriesNr'') of
                                                      (Just seriesDesc''', Just seriesNr''') -> return $ seriesDesc''' ++ "_" ++ seriesNr'''
                                                      _                                      -> throwM $ DicomMissingSeriesDescription f
        _                                  -> throwM $ DicomMissingSeriesDescription f
    -}

    case seriesDesc of
        Right seriesDesc' -> do let seriesDesc'' = unescapeEntities <$> parseSingleMatch pSeriesDescription seriesDesc'
                                case seriesDesc'' of
                                      Just seriesDesc''' -> return seriesDesc'''
                                      _                  -> throwM $ DicomMissingSeriesDescription f
        _                                  -> throwM $ DicomMissingSeriesDescription f

readDicomMetadata :: FilePath -> IO (Either String DicomFile)
readDicomMetadata fileName = do
    dump <- dcmDump fileName

    case dump of
        Left e      -> return $ Left e
        Right dump' -> let parseHere p = (unescapeEntities <$> parseSingleMatch p dump') in
                            return $ Right $ DicomFile
                                    fileName
                                    (parseHere pPatientName)
                                    (parseHere pPatientID)
                                    (parseHere pPatientBirthDate)
                                    (parseHere pPatientSex)
                                    (parseHere pPatientAge)
                                    (parseHere pPatientWeight)
                                    (parseHere pPatientPosition)

                                    (parseHere pStudyDate)
                                    (parseHere pStudyTime)
                                    (parseHere pStudyDescription)
                                    (parseHere pStudyInstanceID)
                                    (parseHere pStudyID)

                                    (parseHere pSeriesDate)
                                    (parseHere pSeriesTime)
                                    (parseHere pSeriesDescription)
                                    (parseHere pSeriesInstanceUID)
                                    (parseHere pSeriesNumber)
                                    (parseHere pCSASeriesHeaderType)
                                    (parseHere pCSASeriesHeaderVersion)
                                    (parseHere pCSASeriesHeaderInfo)
                                    (parseHere pSeriesWorkflowStatus)

                                    (parseHere pMediaStorageSOPInstanceUID)
                                    (parseHere pInstanceCreationDate)
                                    (parseHere pInstanceCreationTime)
                                    (parseHere pSOPInstanceUID)
                                    (parseHere pStudyInstanceUID)
                                    (parseHere pInstanceNumber)

                                    (parseHere pInstitutionName)
                                    (parseHere pInstitutionAddress)
                                    (parseHere pInstitutionalDepartmentName)

                                    (parseHere pReferringPhysicianName)

                                    (parseHere pManufacturerModelName)
                                    (parseHere pManufacturer)

                                    (parseHere pSequenceName)

                                    (parseHere pStationName)

getDicomFilesInDirectory :: String -> FilePath -> IO [FilePath]
getDicomFilesInDirectory suffix dir = filter (isLowerSuffix suffix) <$> getFilesInDirectory dir
  where
    isLowerSuffix a b = map toLower a `isSuffixOf ` map toLower b

    getFilesInDirectory :: FilePath -> IO [FilePath]
    getFilesInDirectory d = map (d </>) <$> filter (not . (`elem` [".", ".."])) <$> getDirectoryContents d >>= filterM doesFileExist


groupDicomFiles :: [DicomFile -> Bool]
                -> [DicomFile -> Maybe String]
                -> [DicomFile -> Maybe String]
                -> [DicomFile] -> [[DicomFile]]
groupDicomFiles instrumentFilters titleFields datasetFields files = map snd <$> groupBy ((==) `on` fst) (sortOn fst $ map toTuple files')

  where

    files' = filter (allTrue instrumentFilters) files

    allTrue :: [a -> Bool] -> a -> Bool
    allTrue fs x = and [ f x | f <- fs]

    toTuple file = ((titleFields <*> [file], datasetFields <*> [file]), file)

-- https://ghc.haskell.org/trac/ghc/ticket/9004
sortOn :: Ord b => (a -> b) -> [a] -> [a]
sortOn f = map snd . sortBy (comparing fst)
                   . map (\x -> let y = f x in y `seq` (y, x))
