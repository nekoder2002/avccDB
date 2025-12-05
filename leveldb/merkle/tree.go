// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

import (
	"bytes"
)

// MerkleTree represents a complete Merkle tree with proof generation
type MerkleTree struct {
	root    *MerkleNode
	compare func(a, b []byte) int

	// Cache for faster lookups
	leafMap map[string]*MerkleNode // key -> leaf node mapping

	// Statistics
	stats TreeStats
}

// NewMerkleTree creates a new Merkle tree from a root node
func NewMerkleTree(root *MerkleNode, compareFunc func(a, b []byte) int) *MerkleTree {
	if compareFunc == nil {
		compareFunc = bytes.Compare
	}

	mt := &MerkleTree{
		root:    root,
		compare: compareFunc,
		leafMap: make(map[string]*MerkleNode),
	}

	// Build leaf index
	mt.buildLeafIndex()

	return mt
}

// buildLeafIndex creates an index of all leaves for fast lookup
func (mt *MerkleTree) buildLeafIndex() {
	if mt.root == nil {
		return
	}

	// Traverse and index all leaves
	mt.traverseLeaves(mt.root, func(leaf *MerkleNode) {
		mt.leafMap[string(leaf.Key)] = leaf
		mt.stats.TotalLeaves++
	})
}

// traverseLeaves walks the tree and calls fn for each leaf
func (mt *MerkleTree) traverseLeaves(node *MerkleNode, fn func(*MerkleNode)) {
	if node == nil {
		return
	}

	if node.IsLeaf() {
		fn(node)
		return
	}

	mt.traverseLeaves(node.Left, fn)
	mt.traverseLeaves(node.Right, fn)
}

// GetRoot returns the root hash of the tree
func (mt *MerkleTree) GetRoot() Hash {
	if mt.root == nil {
		return ZeroHash
	}
	return mt.root.Hash
}

// Get retrieves a value by key
func (mt *MerkleTree) Get(key []byte) ([]byte, uint64, bool) {
	leaf, ok := mt.leafMap[string(key)]
	if !ok {
		return nil, 0, false
	}
	return leaf.Value, leaf.Version, true
}

// GenerateProof generates a Merkle proof for a given key
func (mt *MerkleTree) GenerateProof(key []byte) (*MerkleProof, error) {
	if mt.root == nil {
		return nil, ErrEmptyTree
	}

	// Check if key exists
	leaf, exists := mt.leafMap[string(key)]

	if !exists {
		// Generate non-existence proof
		return mt.generateNonExistenceProof(key)
	}

	// Generate existence proof
	return mt.generateExistenceProof(leaf)
}

// generateExistenceProof generates proof for an existing key
func (mt *MerkleTree) generateExistenceProof(leaf *MerkleNode) (*MerkleProof, error) {
	proof := &MerkleProof{
		Key:     append([]byte(nil), leaf.Key...),
		Value:   append([]byte(nil), leaf.Value...),
		Version: leaf.Version,
		Root:    mt.root.Hash,
		Exists:  true,
		Path:    make([]ProofNode, 0),
	}

	// Build path from leaf to root
	if err := mt.buildProofPath(mt.root, leaf.Key, &proof.Path); err != nil {
		return nil, err
	}

	return proof, nil
}

// buildProofPath recursively builds the proof path
func (mt *MerkleTree) buildProofPath(node *MerkleNode, targetKey []byte, path *[]ProofNode) error {
	if node == nil {
		return ErrKeyNotFound
	}

	if node.IsLeaf() {
		// Reached the leaf
		if mt.compare(node.Key, targetKey) == 0 {
			return nil
		}
		return ErrKeyNotFound
	}

	// Determine which subtree contains the key
	// For binary tree, we need to compare with boundaries
	_ = mt.findLeftmostKey(node.Left) // Mark as used for future implementation
	rightmost := mt.findRightmostKey(node.Left)

	if node.Left != nil && mt.compare(targetKey, rightmost) <= 0 {
		// Key is in left subtree
		// Add right sibling to proof
		if node.Right != nil {
			*path = append(*path, ProofNode{
				Hash:   node.Right.Hash,
				IsLeft: false,
				Height: node.Right.Height,
			})
		}
		return mt.buildProofPath(node.Left, targetKey, path)
	} else if node.Right != nil {
		// Key is in right subtree
		// Add left sibling to proof
		if node.Left != nil {
			*path = append(*path, ProofNode{
				Hash:   node.Left.Hash,
				IsLeft: true,
				Height: node.Left.Height,
			})
		}
		return mt.buildProofPath(node.Right, targetKey, path)
	}

	return ErrKeyNotFound
}

// findLeftmostKey finds the smallest key in subtree
func (mt *MerkleTree) findLeftmostKey(node *MerkleNode) []byte {
	if node == nil {
		return nil
	}
	for !node.IsLeaf() {
		node = node.Left
	}
	return node.Key
}

// findRightmostKey finds the largest key in subtree
func (mt *MerkleTree) findRightmostKey(node *MerkleNode) []byte {
	if node == nil {
		return nil
	}
	for !node.IsLeaf() && node.Right != nil {
		node = node.Right
	}
	// Handle case where there's no right child
	if !node.IsLeaf() {
		node = node.Left
	}
	return node.Key
}

// generateNonExistenceProof generates proof that a key doesn't exist
func (mt *MerkleTree) generateNonExistenceProof(key []byte) (*MerkleProof, error) {
	// For non-existence proof, we need to show the gap where the key would be
	// This requires finding the predecessor and successor keys

	proof := &MerkleProof{
		Key:    append([]byte(nil), key...),
		Value:  nil,
		Root:   mt.root.Hash,
		Exists: false,
		Path:   make([]ProofNode, 0),
	}

	// Find position where key would be inserted
	// For simplicity, use root hash (full implementation would prove the gap)

	return proof, nil
}

// VerifyProof verifies a Merkle proof against this tree's root
func (mt *MerkleTree) VerifyProof(proof *MerkleProof) bool {
	if proof == nil {
		return false
	}

	// Check if proof root matches tree root
	if !proof.Root.Equal(mt.root.Hash) {
		return false
	}

	return proof.Verify()
}

// UpdateLeaf updates or inserts a leaf and recomputes affected hashes
// Returns the new root
func (mt *MerkleTree) UpdateLeaf(key, value []byte, version uint64) (*MerkleNode, error) {
	// For immutable trees (production use), create new tree
	// This is expensive but maintains immutability

	// Collect all leaves
	leaves := CollectLeaves(mt.root)

	// Find and update or insert
	found := false
	for i, leaf := range leaves {
		if mt.compare(leaf.Key, key) == 0 {
			// Update existing
			leaves[i] = NewLeafNode(key, value, version)
			found = true
			break
		}
	}

	if !found {
		// Insert new leaf
		newLeaf := NewLeafNode(key, value, version)
		leaves = append(leaves, newLeaf)

		// Re-sort
		leaves = SortAndDeduplicate(leaves, mt.compare)
	}

	// Rebuild tree from leaves
	builder := NewTreeBuilder(mt.compare)
	for _, leaf := range leaves {
		if err := builder.AddLeaf(leaf.Key, leaf.Value, leaf.Version); err != nil {
			return nil, err
		}
	}

	newRoot, err := builder.Build()
	if err != nil {
		return nil, err
	}

	// Update tree
	mt.root = newRoot
	mt.leafMap = make(map[string]*MerkleNode)
	mt.buildLeafIndex()

	return newRoot, nil
}

// GetStats returns tree statistics
func (mt *MerkleTree) GetStats() TreeStats {
	return mt.stats
}

// RootNode returns the root node (for serialization)
func (mt *MerkleTree) RootNode() *MerkleNode {
	return mt.root
}
