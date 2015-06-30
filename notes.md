
Example of manually removing a study directory from the imagetrove-uploader internal database:

    $ cd imagetrove-uploader
    $ cabal repl

    *Data.Dicom> :l src/Network/ImageTrove/Acid.hs

    *Network.ImageTrove.Acid> acid <- openLocalStateFrom "state_sample_config_files_CAI_all.conf" (KeyValue Map.empty)

    *Network.ImageTrove.Acid> m <- loadMap acid "state_sample_config_files_CAI_all.conf"

    *Network.ImageTrove.Acid> filter (\(k, _) -> k == "store-transfer_1.3.12.2.1107.5.2.34.18975.30000015012901405532800000007") (Map.toList m)
    [("store-transfer_1.3.12.2.1107.5.2.34.18975.30000015012901405532800000007",2015-06-08 03:47:24 AEST)]
           
    *Network.ImageTrove.Acid> deleteLastUpdate acid "state_sample_config_files_CAI_all.conf" "store-transfer_1.3.12.2.1107.5.2.34.18975.30000015012901405532800000007"

    *Network.ImageTrove.Acid> closeAcidState acid
