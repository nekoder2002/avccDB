// Copyright (c) 2024 BVMT Implementation
// Use of this source code is governed by a BSD-style license

package vcs

import (
	"sync"

	"github.com/alinush/go-mcl"
	"github.com/syndtr/goleveldb/leveldb/merkle"
	"github.com/syndtr/goleveldb/leveldb/vcs/asvc"
)

// BVMT represents the Batch Version Merkle Tree structure
type BVMT struct {
	// Core data structures
	batches           []*BatchMerkleTree // All batches with their Merkle trees
	batchRoots        []merkle.Hash      // Root hashes of all batches (v0, v1, ..., vi)
	vectorCommitment  *VBUC              // Vector commitment for batch roots
	vectorDigest      mcl.G1             // Current vector commitment digest
	currentBatchIndex uint64             // Current batch index (number of batches)

	// Vector commitment proofs
	proofs        BucProofAll          // Aggregated proofs for all batches
	auxiliaryData [][][]asvc.UpdateReq // Auxiliary data for vector commitment updates

	// Configuration
	comparer func([]byte, []byte) int // Key comparison function
	vbucL    uint8                    // VBUC parameter L (N = 2^L)

	// Statistics
	stats BVMTStats

	// Concurrency control
	mu sync.RWMutex // Protects all fields
}

// NewBVMT creates a new BVMT instance
// L: VBUC parameter, determines max capacity N = 2^L
// comparer: Key comparison function (nil for default bytes.Compare)
func NewBVMT(L uint8, comparer func([]byte, []byte) int) *BVMT {
	if comparer == nil {
		comparer = compareBytes
	}

	bvmt := &BVMT{
		batches:           make([]*BatchMerkleTree, 0),
		batchRoots:        make([]merkle.Hash, 0),
		vectorCommitment:  &VBUC{},
		currentBatchIndex: 0,
		comparer:          comparer,
		vbucL:             L,
		stats:             BVMTStats{},
	}

	// Initialize VBUC
	bvmt.vectorCommitment.Init(L)
	bvmt.auxiliaryData = bvmt.vectorCommitment.InitAux()

	return bvmt
}

// AddBatch adds a new batch of versioned KV pairs to BVMT
// Returns the batch index if successful
func (bvmt *BVMT) AddBatch(kvPairs []*VersionedKVPair) (uint64, error) {
	bvmt.mu.Lock()
	defer bvmt.mu.Unlock()

	// Validation
	if len(kvPairs) == 0 {
		return 0, ErrEmptyBatch
	}

	// Check capacity
	maxCapacity := uint64(1) << bvmt.vbucL
	if bvmt.currentBatchIndex >= maxCapacity {
		return 0, ErrVectorCapacityExceeded
	}

	// Create new batch
	batchID := bvmt.currentBatchIndex
	batch := NewBatchMerkleTree(batchID)

	// Add KV pairs to batch
	for _, kv := range kvPairs {
		if kv == nil || len(kv.Key) == 0 {
			return 0, ErrInvalidKeyOrder
		}
		batch.AddKVPair(kv.Clone())
	}

	// Build Merkle tree for this batch
	if err := batch.BuildTree(bvmt.comparer); err != nil {
		return 0, err
	}

	// Get batch root hash
	rootHash := batch.RootHash

	// Add batch to storage
	bvmt.batches = append(bvmt.batches, batch)
	bvmt.batchRoots = append(bvmt.batchRoots, rootHash)

	// Update vector commitment
	if err := bvmt.updateVectorCommitment(batchID, rootHash); err != nil {
		// Rollback on error
		bvmt.batches = bvmt.batches[:len(bvmt.batches)-1]
		bvmt.batchRoots = bvmt.batchRoots[:len(bvmt.batchRoots)-1]
		return 0, err
	}

	// Update statistics
	bvmt.stats.UpdateStats(uint64(len(kvPairs)))

	// Increment batch index
	bvmt.currentBatchIndex++

	return batchID, nil
}

// updateVectorCommitment updates the vector commitment with a new batch root
func (bvmt *BVMT) updateVectorCommitment(batchIndex uint64, rootHash merkle.Hash) error {
	// Convert all batch roots to Fr vector
	vector := vectorToFrArray(bvmt.batchRoots)

	// Pad vector to full capacity N for VBUC
	maxCapacity := uint64(1) << bvmt.vbucL
	if uint64(len(vector)) < maxCapacity {
		padded := make([]mcl.Fr, maxCapacity)
		copy(padded, vector)
		// Zero-fill the rest
		for i := uint64(len(vector)); i < maxCapacity; i++ {
			padded[i].Clear()
		}
		vector = padded
	}

	// SIMPLIFIED: Always use Commit + OpenAll for consistency
	// This avoids issues with incremental updates via UpdateAll
	bvmt.vectorDigest = bvmt.vectorCommitment.Commit(vector)
	bvmt.proofs = bvmt.vectorCommitment.OpenAll(vector)

	return nil
}

// GenerateProof generates a proof for a specific key and version
// If version is 0, returns proof for the latest version of the key
func (bvmt *BVMT) GenerateProof(key []byte, version uint64) (*BVMTProof, error) {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	// Search for the key in batches (newest to oldest for efficiency)
	var foundBatch *BatchMerkleTree
	var foundKV *VersionedKVPair
	var batchIndex uint64
	var leafIndex int

	for i := len(bvmt.batches) - 1; i >= 0; i-- {
		batch := bvmt.batches[i]
		kv, ok := batch.FindKVPair(key)
		if ok {
			// Check version match
			if version == 0 || kv.Version == version {
				foundBatch = batch
				foundKV = kv
				batchIndex = uint64(i)
				// Find the index of this key in the KVPairs array
				idx, exists := batch.KVIndex[string(key)]
				if !exists {
					return nil, ErrKeyNotFound
				}
				leafIndex = int(idx)
				break
			}
		}
	}

	if foundKV == nil {
		return nil, ErrKeyNotFound
	}

	// Generate Merkle proof within the batch using leaf index
	merkleProof, err := foundBatch.Tree.GenerateProof(leafIndex)
	if err != nil {
		return nil, err
	}

	// Get batch root
	batchRoot := bvmt.batchRoots[batchIndex]

	// Query vector commitment proof
	vectorProof := bvmt.vectorCommitment.Query(batchIndex, bvmt.proofs, bvmt.auxiliaryData)

	// Construct BVMT proof
	proof := NewBVMTProof(foundKV, batchIndex)
	proof.MerkleProof = merkleProof
	proof.BatchRoot = batchRoot
	proof.VectorProof = vectorProof
	proof.VectorDigest = bvmt.vectorDigest

	return proof, nil
}

// VerifyProof verifies a BVMT proof using dual-layer verification
func (bvmt *BVMT) VerifyProof(proof *BVMTProof) bool {
	if proof == nil || proof.MerkleProof == nil {
		return false
	}

	// Compute leaf hash from the KV pair
	leafHash := merkle.HashLeaf(proof.KVPair.Key, proof.KVPair.Value)

	// Layer 1: Verify Merkle proof within the batch
	if !proof.MerkleProof.Verify(leafHash) {
		return false
	}

	// Check if Merkle proof root matches batch root
	if !proof.MerkleProof.Root.Equal(proof.BatchRoot) {
		return false
	}

	// Layer 2: Verify batch root is at the correct position in vector commitment
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	if proof.BatchIndex >= uint64(len(bvmt.batchRoots)) {
		return false
	}
	if !bvmt.batchRoots[proof.BatchIndex].Equal(proof.BatchRoot) {
		return false
	}

	// Layer 3: Verify vector commitment proof
	val := asvc.Val{
		Index: proof.BatchIndex,
		Y:     hashToFr(proof.BatchRoot),
	}

	return bvmt.vectorCommitment.VerifySingle(proof.VectorDigest, proof.VectorProof, val)
}

// VerifyBatchProofs verifies multiple proofs efficiently using batch verification
func (bvmt *BVMT) VerifyBatchProofs(proofs []*BVMTProof) bool {
	if len(proofs) == 0 {
		return false
	}

	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	// Step 1: Group proofs by batch index
	batchGroups := make(map[uint64][]*BVMTProof)
	for _, proof := range proofs {
		if proof == nil {
			return false
		}
		batchGroups[proof.BatchIndex] = append(batchGroups[proof.BatchIndex], proof)
	}

	// Step 2: Verify Merkle proofs for each batch
	for batchIdx, batchProofs := range batchGroups {
		if batchIdx >= uint64(len(bvmt.batchRoots)) {
			return false
		}

		batchRoot := bvmt.batchRoots[batchIdx]
		for _, proof := range batchProofs {
			// Compute leaf hash from the KV pair
			leafHash := merkle.HashLeaf(proof.KVPair.Key, proof.KVPair.Value)

			// Verify Merkle proof
			if !proof.MerkleProof.Verify(leafHash) {
				return false
			}
			// Verify root matches
			if !proof.MerkleProof.Root.Equal(batchRoot) {
				return false
			}
			if !proof.BatchRoot.Equal(batchRoot) {
				return false
			}
		}
	}

	// Step 3: Prepare for aggregated vector commitment verification
	indices := make([]uint64, 0, len(batchGroups))
	vectorProofs := make([]BucProofSingle, 0, len(batchGroups))

	for batchIdx := range batchGroups {
		indices = append(indices, batchIdx)
		// Use the first proof's vector proof (all should be the same for the same batch)
		vectorProofs = append(vectorProofs, batchGroups[batchIdx][0].VectorProof)
	}

	// Step 4: Aggregate vector commitment proofs
	aggProof := bvmt.vectorCommitment.Aggregate(indices, vectorProofs)

	// Step 5: Prepare aggregated values
	aggvs := make([][][]mcl.Fr, len(indices))
	for i, batchIdx := range indices {
		aggvs[i] = make([][]mcl.Fr, 1)
		aggvs[i][0] = make([]mcl.Fr, 1)
		aggvs[i][0][0] = hashToFr(bvmt.batchRoots[batchIdx])
	}

	// Step 6: Verify aggregated proof
	return bvmt.vectorCommitment.VerifyAggregation(bvmt.vectorDigest, aggProof, aggvs)
}

// GetBatch retrieves a batch by index
func (bvmt *BVMT) GetBatch(batchIndex uint64) (*BatchMerkleTree, error) {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	if batchIndex >= uint64(len(bvmt.batches)) {
		return nil, ErrBatchNotFound
	}

	return bvmt.batches[batchIndex], nil
}

// GetStats returns current statistics
func (bvmt *BVMT) GetStats() BVMTStats {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	return bvmt.stats
}

// GetCurrentBatchIndex returns the current batch index
func (bvmt *BVMT) GetCurrentBatchIndex() uint64 {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	return bvmt.currentBatchIndex
}

// GetVectorDigest returns the current vector commitment digest
func (bvmt *BVMT) GetVectorDigest() mcl.G1 {
	bvmt.mu.RLock()
	defer bvmt.mu.RUnlock()

	return bvmt.vectorDigest
}
