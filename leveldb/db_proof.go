package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/dbkey"
	"github.com/syndtr/goleveldb/leveldb/merkle"
)

// ProofSource indicates where the data was found
type ProofSource int

// DBProof contains the complete Merkle proof chain for a key-value pair.
// The proof structure supports data from both MemDB and SST files.
//
// Proof verification chain:
//  1. DataProof: Proves key-value exists in the data source (MemDB or SST)
//  2. LayerProof: Proves the data source is part of its layer
//  3. MasterProof: Proves the layer is part of the database state
//
// For MemDB data:
//
//	DataProof = MemDB's internal Merkle proof
//	LayerProof = Proof that MemDB root is in the memory layer
//
// For SST data:
//
//	DataProof = SST's internal Merkle proof
//	LayerProof = Proof that SST root is in its level
type DBProof struct {
	DataProof *merkle.MerkleProof

	LayerProof *merkle.MerkleProof

	MasterProof *merkle.MerkleProof
}

// Verify verifies the complete Merkle proof chain for a key-value pair
func (p *DBProof) Verify(key []byte, version uint64, value []byte) bool {
	uvkey := dbkey.MakeUVKey(nil, key, version)
	leafHash := merkle.HashLeaf(uvkey, value)
	dataProof := p.DataProof
	return dataProof.Verify(leafHash) && p.LayerProof.Verify(dataProof.Root) && p.MasterProof.Verify(p.LayerProof.Root)
}
