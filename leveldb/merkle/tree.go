// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

// MerkleTree represents a Merkle tree built from sorted hashes
//
// SIMPLIFIED FOR SORTED DATA (SSTable):
// - Only stores hashes, not key-value pairs
// - No comparator needed - append-only in order
// - Array-based structure with O(1) index access
// - Proof generation uses simple index arithmetic
//
// Benefits:
// - Minimal memory: only hashes, no keys/values
// - Fast: O(log n) proof generation
// - Simple: no tree traversal, just array operations
type MerkleTree struct {
	// Leaf hashes in order
	leafHashes []Hash

	// Tree levels built bottom-up
	// levels[0] = leaf hashes
	// levels[1] = parent hashes
	// levels[n] = root hash
	levels [][]Hash

	// Root hash (cached)
	rootHash Hash

	// Statistics
	stats TreeStats
}

// NewMerkleTree creates a new Merkle tree from leaf hashes
func NewMerkleTree(leafHashes []Hash) *MerkleTree {
	if len(leafHashes) == 0 {
		return &MerkleTree{
			rootHash: ZeroHash,
		}
	}

	mt := &MerkleTree{
		leafHashes: leafHashes,
		stats: TreeStats{
			TotalLeaves: len(leafHashes),
		},
	}

	// Build tree levels
	mt.buildLevels()

	return mt
}

// buildLevels builds all tree levels from bottom to top
func (mt *MerkleTree) buildLevels() {
	mt.levels = make([][]Hash, 0, 8)
	mt.levels = append(mt.levels, mt.leafHashes)

	// Build each level by pairing hashes from previous level
	currentLevel := mt.leafHashes
	for len(currentLevel) > 1 {
		nextLevel := make([]Hash, 0, (len(currentLevel)+1)/2)

		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// Pair: hash the two siblings
				parent := HashInternal(currentLevel[i], currentLevel[i+1])
				nextLevel = append(nextLevel, parent)
			} else {
				// Odd one out: promote to next level
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		mt.levels = append(mt.levels, nextLevel)
		currentLevel = nextLevel
	}

	// Root is the only element in top level
	mt.rootHash = currentLevel[0]
	mt.stats.TreeHeight = len(mt.levels) - 1
}

// GetRoot returns the root hash
func (mt *MerkleTree) GetRoot() Hash {
	return mt.rootHash
}

// GenerateProof generates a Merkle proof for the leaf at given index
func (mt *MerkleTree) GenerateProof(leafIndex int) (*MerkleProof, error) {
	if leafIndex < 0 || leafIndex >= len(mt.leafHashes) {
		return nil, ErrKeyNotFound
	}

	proof := &MerkleProof{
		Root:   mt.rootHash,
		Exists: true,
		Path:   make([]ProofNode, 0, mt.stats.TreeHeight),
	}

	// Build proof path by walking up the tree
	currentIdx := leafIndex
	for level := 0; level < len(mt.levels)-1; level++ {
		currentLevelHashes := mt.levels[level]

		// Find sibling
		isLeft := (currentIdx % 2) == 0
		var siblingIdx int

		if isLeft {
			siblingIdx = currentIdx + 1
		} else {
			siblingIdx = currentIdx - 1
		}

		// Add sibling to proof if it exists
		if siblingIdx >= 0 && siblingIdx < len(currentLevelHashes) {
			proof.Path = append(proof.Path, ProofNode{
				Hash:   currentLevelHashes[siblingIdx],
				IsLeft: !isLeft, // Sibling's position relative to us
				Height: int32(level),
			})
		}

		// Move to parent
		currentIdx = currentIdx / 2
	}

	return proof, nil
}

// GetStats returns tree statistics
func (mt *MerkleTree) GetStats() TreeStats {
	return mt.stats
}
