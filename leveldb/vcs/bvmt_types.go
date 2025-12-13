// Copyright (c) 2024 BVMT Implementation
// Use of this source code is governed by a BSD-style license

package vcs

import (
	"bytes"
	"time"

	"github.com/alinush/go-mcl"
	"github.com/syndtr/goleveldb/leveldb/merkle"
)

// VersionedKVPair represents a key-value pair with version information
type VersionedKVPair struct {
	Key        []byte // Key data
	Value      []byte // Value data
	Version    uint64 // Version number
	BatchIndex uint64 // Index of the batch this KV pair belongs to
}

// NewVersionedKVPair creates a new versioned KV pair
func NewVersionedKVPair(key, value []byte, version uint64) *VersionedKVPair {
	return &VersionedKVPair{
		Key:     append([]byte(nil), key...),
		Value:   append([]byte(nil), value...),
		Version: version,
	}
}

// Clone creates a copy of the KV pair
func (kv *VersionedKVPair) Clone() *VersionedKVPair {
	return &VersionedKVPair{
		Key:        append([]byte(nil), kv.Key...),
		Value:      append([]byte(nil), kv.Value...),
		Version:    kv.Version,
		BatchIndex: kv.BatchIndex,
	}
}

// BatchMerkleTree represents a batch with its Merkle tree
type BatchMerkleTree struct {
	BatchID   uint64             // Unique batch identifier
	Tree      *merkle.MerkleTree // Merkle tree for this batch
	KVPairs   []*VersionedKVPair // KV pairs in this batch
	RootHash  merkle.Hash        // Batch Merkle root hash
	Timestamp uint64             // Batch creation timestamp (Unix nano)
	KVIndex   map[string]uint64  // Map from key to index in KVPairs
}

// NewBatchMerkleTree creates a new batch Merkle tree
func NewBatchMerkleTree(batchID uint64) *BatchMerkleTree {
	return &BatchMerkleTree{
		BatchID:   batchID,
		KVPairs:   make([]*VersionedKVPair, 0),
		Timestamp: uint64(time.Now().UnixNano()),
		KVIndex:   make(map[string]uint64),
	}
}

// AddKVPair adds a KV pair to the batch (must be in sorted order)
func (b *BatchMerkleTree) AddKVPair(kv *VersionedKVPair) {
	idx := uint64(len(b.KVPairs))
	kv.BatchIndex = b.BatchID
	b.KVPairs = append(b.KVPairs, kv)
	b.KVIndex[string(kv.Key)] = idx
}

// FindKVPair finds a KV pair by key
func (b *BatchMerkleTree) FindKVPair(key []byte) (*VersionedKVPair, bool) {
	idx, ok := b.KVIndex[string(key)]
	if !ok {
		return nil, false
	}
	return b.KVPairs[idx], true
}

// BuildTree builds the Merkle tree from KV pairs
func (b *BatchMerkleTree) BuildTree(comparer func(a, b []byte) int) error {
	if len(b.KVPairs) == 0 {
		return ErrEmptyBatch
	}

	// Sort KV pairs by key (and version for same keys)
	sortKVPairs(b.KVPairs, comparer)

	// Build Merkle tree using TreeBuilder
	builder := merkle.NewTreeBuilder(comparer)
	for _, kv := range b.KVPairs {
		// Note: AddLeaf only takes key and value, version is handled separately
		if err := builder.AddLeaf(kv.Key, kv.Value); err != nil {
			return err
		}
	}

	// Validate the builder has leaves
	if _, err := builder.Build(); err != nil {
		return err
	}

	// Create leaf hashes from the tree
	leafHashes := make([]merkle.Hash, len(b.KVPairs))
	for i, kv := range b.KVPairs {
		leafHashes[i] = merkle.HashLeaf(kv.Key, kv.Value)
	}

	b.Tree = merkle.NewMerkleTree(leafHashes)
	b.RootHash = b.Tree.GetRoot()
	return nil
}

// sortKVPairs sorts KV pairs by key (ascending) and version (descending for same keys)
func sortKVPairs(pairs []*VersionedKVPair, comparer func(a, b []byte) int) {
	// Use insertion sort for stability and simplicity
	for i := 1; i < len(pairs); i++ {
		j := i
		for j > 0 {
			cmp := comparer(pairs[j-1].Key, pairs[j].Key)
			if cmp < 0 {
				// keys are in order (j-1 < j), don't swap
				break
			}
			if cmp == 0 {
				// Same key: check version (want descending order, so higher version first)
				if pairs[j-1].Version >= pairs[j].Version {
					// versions are in order or equal, don't swap
					break
				}
				// pairs[j-1].Version < pairs[j].Version, need to swap to get descending
			}
			// cmp > 0 or (cmp == 0 && need to swap), so swap
			pairs[j-1], pairs[j] = pairs[j], pairs[j-1]
			j--
		}
	}
}

// BVMTStats contains statistics about the BVMT structure
type BVMTStats struct {
	TotalBatches     uint64 // Total number of batches
	TotalKVPairs     uint64 // Total number of KV pairs across all batches
	TotalTreeNodes   uint64 // Total number of Merkle tree nodes
	AverageBatchSize uint64 // Average number of KV pairs per batch
}

// UpdateStats updates statistics after adding a batch
func (s *BVMTStats) UpdateStats(batchSize uint64) {
	s.TotalBatches++
	s.TotalKVPairs += batchSize
	if s.TotalBatches > 0 {
		s.AverageBatchSize = s.TotalKVPairs / s.TotalBatches
	}
}

// BVMTProof represents a proof for a versioned KV pair in BVMT
type BVMTProof struct {
	KVPair       *VersionedKVPair    // The KV pair being proved
	BatchIndex   uint64              // Index of the batch containing the KV pair
	MerkleProof  *merkle.MerkleProof // Merkle proof within the batch
	BatchRoot    merkle.Hash         // Root hash of the batch
	VectorProof  BucProofSingle      // Vector commitment proof for the batch root
	VectorDigest mcl.G1              // Vector commitment digest
}

// NewBVMTProof creates a new BVMT proof
func NewBVMTProof(kv *VersionedKVPair, batchIndex uint64) *BVMTProof {
	return &BVMTProof{
		KVPair:     kv.Clone(),
		BatchIndex: batchIndex,
	}
}

// hashToFr converts a merkle.Hash to mcl.Fr
// Uses the first 32 bytes of the hash as Fr value
func hashToFr(h merkle.Hash) mcl.Fr {
	var fr mcl.Fr
	// SetLittleEndian is typically used for setting from bytes
	// We use the hash bytes directly
	fr.SetLittleEndian(h[:])
	return fr
}

// frToBytes converts mcl.Fr to bytes for hashing
func frToBytes(fr mcl.Fr) []byte {
	return fr.Serialize()
}

// vectorToFrArray converts batch roots to Fr array for vector commitment
func vectorToFrArray(roots []merkle.Hash) []mcl.Fr {
	result := make([]mcl.Fr, len(roots))
	for i, root := range roots {
		result[i] = hashToFr(root)
	}
	return result
}

// compareBytes is the default key comparator
func compareBytes(a, b []byte) int {
	return bytes.Compare(a, b)
}
