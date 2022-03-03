package surfstore

import (
	context "context"
	"fmt"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

var mutexBlock sync.Mutex

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	mutexBlock.Lock()
	defer mutexBlock.Unlock()

	if block, ok := bs.BlockMap[blockHash.Hash]; ok {
		return &Block{
			BlockData: block.BlockData,
			BlockSize: block.BlockSize,
		}, nil
	} else {
		return nil, fmt.Errorf("could not find block with hash %v", blockHash.Hash)
	}

}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	mutexBlock.Lock()
	defer mutexBlock.Unlock()

	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = &Block{
		BlockData: block.BlockData,
		BlockSize: block.BlockSize,
	}
	return &Success{
		Flag: true,
	}, nil

}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	mutexBlock.Lock()
	defer mutexBlock.Unlock()

	var outHashes []string
	for _, blockHashIn := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[blockHashIn]; ok {
			outHashes = append(outHashes, blockHashIn)
		}
	}
	return &BlockHashes{Hashes: outHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
