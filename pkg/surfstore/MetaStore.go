package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

var mutex sync.Mutex

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	mutex.Lock()
	defer mutex.Unlock()

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	mutex.Lock()
	defer mutex.Unlock()

	toUpdateVersion := fileMetaData.Version
	name := fileMetaData.Filename

	if thisFileMetaData, ok := m.FileMetaMap[name]; ok {
		olderVersion := thisFileMetaData.Version
		// if file is deleted
		if m.FileMetaMap[name].BlockHashList[0] == "0" {
			thisFileMetaData.Version = olderVersion + 1
			return &Version{Version: olderVersion + 1}, nil
		}
		// if file existed and the new version number is exactly one greater than the current version number
		if olderVersion+1 == toUpdateVersion {
			//update filedata
			thisFileMetaData.Version = toUpdateVersion
			thisFileMetaData.BlockHashList = fileMetaData.BlockHashList
			return &Version{Version: toUpdateVersion}, nil
		} else {
			//return error
			return &Version{Version: -1}, nil
		}
	} else {
		// file does not exist
		m.FileMetaMap[name] = fileMetaData
		return &Version{Version: 1}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	mutex.Lock()
	defer mutex.Unlock()

	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
