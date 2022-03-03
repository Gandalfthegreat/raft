package surfstore

import (
	"io/ioutil"
	"log"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// initialize blockStoreAddr and serverFileInfoMap
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}
	serverFileInfoMap := &FileInfoMap{}
	if err := client.GetFileInfoMap(&serverFileInfoMap.FileInfoMap); err != nil {
		log.Fatal(err)
	}
	files, _ := ioutil.ReadDir("./")
	var filesName []string
	for _, f := range files {
		filesName = append(filesName, f.Name())
	}
	// local file does not exist on remote server
	for _, fileName := range filesName {
		if _, ok := serverFileInfoMap.FileInfoMap[fileName]; !ok {

		}
	}

	blockHash := "1234"
	var block Block
	if err := client.GetBlock(blockHash, blockStoreAddr, &block); err != nil {
		log.Fatal(err)
	}

	log.Print(block.String())
}
