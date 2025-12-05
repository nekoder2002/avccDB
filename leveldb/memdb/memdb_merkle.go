// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package memdb

import (
	"bytes"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/merkle"
)

// MerkleDB wraps DB with Merkle tree support
type MerkleDB struct {
	*DB

	mu sync.RWMutex

	// Merkle tree built from current state
	tree *merkle.MerkleTree

	// Flag to indicate if tree needs rebuild
	dirty bool

	// Cached root hash
	rootHash merkle.Hash

	// Enable versioning
	enableVersioning bool
}

// NewMerkleDB creates a new MemDB with Merkle tree support
func NewMerkleDB(cmp comparer.BasicComparer, capacity int) *MerkleDB {
	return &MerkleDB{
		DB:               New(cmp, capacity),
		dirty:            true,
		enableVersioning: true,
	}
}

// PutWithVersion puts a key-value pair with version number
func (mdb *MerkleDB) PutWithVersion(key, value []byte, version uint64) error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()

	// Store in underlying DB
	// Note: The actual internal key encoding with version is handled at batch level
	if err := mdb.DB.Put(key, value); err != nil {
		return err
	}

	// Mark tree as dirty
	mdb.dirty = true
	return nil
}

// BuildMerkleTree builds or rebuilds the Merkle tree from current state
func (mdb *MerkleDB) BuildMerkleTree() error {
	mdb.mu.Lock()
	defer mdb.mu.Unlock()

	if !mdb.dirty && mdb.tree != nil {
		return nil // Tree is up to date
	}

	// Collect all key-value pairs
	iter := mdb.DB.NewIterator(nil)
	defer iter.Release()

	var pairs []merkle.KVPair
	for iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		value := append([]byte(nil), iter.Value()...)

		// Extract version if present (from internal key encoding)
		version := uint64(0)
		// For now, version extraction is simplified
		// In production, parse from internal key format

		pairs = append(pairs, merkle.KVPair{
			Key:     key,
			Value:   value,
			Version: version,
		})
	}

	if len(pairs) == 0 {
		mdb.tree = nil
		mdb.rootHash = merkle.ZeroHash
		mdb.dirty = false
		return nil
	}

	// Build tree
	root, err := merkle.BuildFromSorted(pairs, bytes.Compare)
	if err != nil {
		return err
	}

	mdb.tree = merkle.NewMerkleTree(root, bytes.Compare)
	mdb.rootHash = mdb.tree.GetRoot()
	mdb.dirty = false

	return nil
}

// GetRootHash returns the Merkle root hash
func (mdb *MerkleDB) GetRootHash() (merkle.Hash, error) {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()

	if mdb.dirty {
		// Need to rebuild - release read lock and acquire write lock
		mdb.mu.RUnlock()
		if err := mdb.BuildMerkleTree(); err != nil {
			mdb.mu.RLock()
			return merkle.ZeroHash, err
		}
		mdb.mu.RLock()
	}

	return mdb.rootHash, nil
}

// GetWithProof retrieves value and generates Merkle proof
func (mdb *MerkleDB) GetWithProof(key []byte) (*merkle.MerkleProof, error) {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()

	// Ensure tree is built
	if mdb.dirty {
		mdb.mu.RUnlock()
		if err := mdb.BuildMerkleTree(); err != nil {
			mdb.mu.RLock()
			return nil, err
		}
		mdb.mu.RLock()
	}

	if mdb.tree == nil {
		return nil, merkle.ErrEmptyTree
	}

	// Generate proof
	return mdb.tree.GenerateProof(key)
}

// GetTree returns the underlying Merkle tree (for testing/debugging)
func (mdb *MerkleDB) GetTree() *merkle.MerkleTree {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()
	return mdb.tree
}

// Stats returns Merkle tree statistics
type MerkleStats struct {
	TreeStats  merkle.TreeStats
	RootHash   merkle.Hash
	IsDirty    bool
	NumEntries int
}

// GetMerkleStats returns statistics about the Merkle tree
func (mdb *MerkleDB) GetMerkleStats() MerkleStats {
	mdb.mu.RLock()
	defer mdb.mu.RUnlock()

	stats := MerkleStats{
		RootHash:   mdb.rootHash,
		IsDirty:    mdb.dirty,
		NumEntries: mdb.DB.Len(),
	}

	if mdb.tree != nil {
		stats.TreeStats = mdb.tree.GetStats()
	}

	return stats
}
