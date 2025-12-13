// Copyright (c) 2024 mLSM Implementation
// MemDB Merkle tree support for in-memory key/value database

package memdb

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/merkle"
)

// MerkleSnapshot represents a snapshot of MemDB's Merkle tree state
// Built lazily when proof is needed
type MerkleSnapshot struct {
	tree     *merkle.MerkleTree
	keyIndex map[string]int // map from key to leaf index
	keys     [][]byte       // ordered keys for lookup
	values   [][]byte       // corresponding values
	root     merkle.Hash
}

// parseVersionedKey extracts ukey and version from internal key
// Internal key format: ukey | version (8 bytes) | seq+type (8 bytes)
func parseVersionedKey(ikey []byte) (ukey []byte, version uint64, ok bool) {
	if len(ikey) < 16 {
		return nil, 0, false
	}
	ukey = ikey[:len(ikey)-16]
	version = binary.LittleEndian.Uint64(ikey[len(ikey)-16 : len(ikey)-8])
	return ukey, version, true
}

// MakeUVKey creates a key with version (ukey | version)
func MakeUVKey(ukey []byte, version uint64) []byte {
	uvkey := make([]byte, len(ukey)+8)
	copy(uvkey, ukey)
	binary.LittleEndian.PutUint64(uvkey[len(ukey):], version)
	return uvkey
}

// BuildMerkleSnapshot builds a Merkle tree snapshot from the current MemDB state
// This is a read-only operation that creates a point-in-time snapshot
func (p *DB) BuildMerkleSnapshot() *MerkleSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.buildMerkleSnapshotLocked()
}

// buildMerkleSnapshotLocked builds snapshot while holding lock
func (p *DB) buildMerkleSnapshotLocked() *MerkleSnapshot {
	if p.n == 0 {
		return &MerkleSnapshot{
			root: merkle.ZeroHash,
		}
	}

	snapshot := &MerkleSnapshot{
		keyIndex: make(map[string]int),
		keys:     make([][]byte, 0, p.n),
		values:   make([][]byte, 0, p.n),
	}

	// Collect all key-value pairs in sorted order using skip list traversal
	leafHashes := make([]merkle.Hash, 0, p.n)

	// Traverse skip list from beginning
	node := p.nodeData[nNext] // First node at level 0
	idx := 0
	for node != 0 {
		// Extract key and value
		kvOffset := p.nodeData[node]
		keyLen := p.nodeData[node+nKey]
		valLen := p.nodeData[node+nVal]

		ikey := make([]byte, keyLen)
		copy(ikey, p.kvData[kvOffset:kvOffset+keyLen])

		value := make([]byte, valLen)
		copy(value, p.kvData[kvOffset+keyLen:kvOffset+keyLen+valLen])

		// Store key and value
		snapshot.keys = append(snapshot.keys, ikey)
		snapshot.values = append(snapshot.values, value)
		snapshot.keyIndex[string(ikey)] = idx

		// Compute leaf hash using uvkey format: H(uvkey || value)
		// uvkey = ukey | version (same format as SST)
		ukey, version, ok := parseVersionedKey(ikey)
		var leafHash merkle.Hash
		if ok {
			uvkey := MakeUVKey(ukey, version)
			leafHash = merkle.HashLeaf(uvkey, value)
		} else {
			// Fallback: use full internal key
			leafHash = merkle.HashLeaf(ikey, value)
		}
		leafHashes = append(leafHashes, leafHash)

		// Move to next node at level 0
		node = p.nodeData[node+nNext]
		idx++
	}

	// Build Merkle tree from leaf hashes
	if len(leafHashes) > 0 {
		snapshot.tree = merkle.NewMerkleTree(leafHashes)
		snapshot.root = snapshot.tree.GetRoot()
	} else {
		snapshot.root = merkle.ZeroHash
	}

	return snapshot
}

// GetRoot returns the Merkle root hash of the snapshot
func (s *MerkleSnapshot) GetRoot() merkle.Hash {
	if s == nil {
		return merkle.ZeroHash
	}
	return s.root
}

// GenerateProof generates a Merkle proof for the given key
// Returns the proof, value, and whether the key exists
func (s *MerkleSnapshot) GenerateProof(key []byte) (*merkle.MerkleProof, []byte, bool) {
	if s == nil || s.tree == nil {
		return nil, nil, false
	}

	idx, exists := s.keyIndex[string(key)]
	if !exists {
		return nil, nil, false
	}

	proof, err := s.tree.GenerateProof(idx)
	if err != nil {
		return nil, nil, false
	}

	return proof, s.values[idx], true
}

// GetWithProof gets value and Merkle proof for a key from MemDB
// This builds a snapshot and generates proof in one operation
func (p *DB) GetWithProof(key []byte) (value []byte, proof *merkle.MerkleProof, root merkle.Hash, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// First check if key exists
	node, exact := p.findGE(key, false)
	if !exact {
		err = ErrNotFound
		return
	}

	// Get the value
	o := p.nodeData[node] + p.nodeData[node+nKey]
	value = make([]byte, p.nodeData[node+nVal])
	copy(value, p.kvData[o:o+p.nodeData[node+nVal]])

	// Build Merkle snapshot
	snapshot := p.buildMerkleSnapshotLocked()
	if snapshot == nil || snapshot.tree == nil {
		// Return value without proof
		return value, nil, merkle.ZeroHash, nil
	}

	// Find key index in snapshot
	idx, exists := snapshot.keyIndex[string(key)]
	if !exists {
		// Key not in snapshot (shouldn't happen)
		return value, nil, snapshot.root, nil
	}

	// Generate proof
	proof, err = snapshot.tree.GenerateProof(idx)
	if err != nil {
		// Return value without proof
		return value, nil, snapshot.root, nil
	}

	return value, proof, snapshot.root, nil
}

// GetMerkleRoot returns the current Merkle root of the MemDB
// This builds a snapshot to compute the root
func (p *DB) GetMerkleRoot() merkle.Hash {
	snapshot := p.BuildMerkleSnapshot()
	return snapshot.GetRoot()
}
