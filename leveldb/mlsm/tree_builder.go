// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package mlsm

import (
	"bytes"
	"errors"
	"sort"
)

// TreeBuilder builds Merkle trees efficiently from sorted key-value pairs
// It uses a bottom-up approach for optimal memory usage
type TreeBuilder struct {
	// Comparer for key comparison
	compare func(a, b []byte) int

	// Leaves stores all leaf nodes before tree construction
	leaves []*MerkleNode

	// Stack for building the tree bottom-up
	nodeStack []*MerkleNode

	// Statistics
	totalNodes  int
	totalLeaves int
	treeHeight  int

	// Options
	enableVersioning bool
}

// NewTreeBuilder creates a new tree builder
func NewTreeBuilder(compareFunc func(a, b []byte) int) *TreeBuilder {
	if compareFunc == nil {
		compareFunc = bytes.Compare
	}
	return &TreeBuilder{
		compare:   compareFunc,
		leaves:    make([]*MerkleNode, 0, 1024),
		nodeStack: make([]*MerkleNode, 0, 32),
	}
}

// AddLeaf adds a leaf node to the builder
func (tb *TreeBuilder) AddLeaf(key, value []byte, version uint64) error {
	// Check if keys are in sorted order
	// For versioned keys with same user key, higher version comes first
	if len(tb.leaves) > 0 {
		lastLeaf := tb.leaves[len(tb.leaves)-1]
		cmp := tb.compare(lastLeaf.Key, key)
		if cmp > 0 {
			return errors.New("mlsm: keys must be added in sorted order")
		} else if cmp == 0 {
			// Same key - check version ordering (higher version should come first)
			if version > lastLeaf.Version {
				return errors.New("mlsm: for same key, higher versions must come first")
			}
			// Allow same key with lower or equal version
		}
	}

	leaf := NewLeafNode(key, value, version)
	tb.leaves = append(tb.leaves, leaf)
	tb.totalLeaves++
	return nil
}

// AddLeaves adds multiple leaves at once
func (tb *TreeBuilder) AddLeaves(leaves []*MerkleNode) error {
	for _, leaf := range leaves {
		if !leaf.IsLeaf() {
			return errors.New("mlsm: only leaf nodes can be added")
		}
		if err := tb.AddLeaf(leaf.Key, leaf.Value, leaf.Version); err != nil {
			return err
		}
	}
	return nil
}

// Build constructs the Merkle tree from all added leaves
// Returns the root node of the tree
func (tb *TreeBuilder) Build() (*MerkleNode, error) {
	if len(tb.leaves) == 0 {
		return nil, ErrEmptyTree
	}

	// Single leaf case
	if len(tb.leaves) == 1 {
		tb.treeHeight = 0
		tb.totalNodes = 1
		return tb.leaves[0], nil
	}

	// Build tree bottom-up using a stack
	// This approach is memory-efficient and produces a balanced tree
	return tb.buildBalancedTree()
}

// buildBalancedTree builds a complete binary tree
func (tb *TreeBuilder) buildBalancedTree() (*MerkleNode, error) {
	// Create initial queue with all leaves
	currentLevel := tb.leaves
	height := int32(0)

	for len(currentLevel) > 1 {
		nextLevel := make([]*MerkleNode, 0, (len(currentLevel)+1)/2)

		// Pair up nodes and create parents
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// We have a pair
				parent := NewInternalNode(currentLevel[i], currentLevel[i+1])
				parent.Height = height + 1
				nextLevel = append(nextLevel, parent)
				tb.totalNodes++
			} else {
				// Odd node out, promote to next level
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		currentLevel = nextLevel
		height++
	}

	tb.treeHeight = int(height)
	tb.totalNodes += len(tb.leaves)
	return currentLevel[0], nil
}

// BuildFromIterator builds a tree from an iterator that provides sorted KV pairs
type KVPair struct {
	Key     []byte
	Value   []byte
	Version uint64
}

// BuildFromSorted builds a tree from pre-sorted KV pairs
func BuildFromSorted(pairs []KVPair, compareFunc func(a, b []byte) int) (*MerkleNode, error) {
	builder := NewTreeBuilder(compareFunc)

	for i := range pairs {
		if err := builder.AddLeaf(pairs[i].Key, pairs[i].Value, pairs[i].Version); err != nil {
			return nil, err
		}
	}

	return builder.Build()
}

// BuildTreeFromHashes builds a Merkle tree from a list of hashes.
// This is used to build layer roots from SST roots, and MasterRoot from layer roots.
// The hashes are treated as leaf nodes, and a balanced binary tree is constructed.
func BuildTreeFromHashes(hashes []Hash) Hash {
	if len(hashes) == 0 {
		return ZeroHash
	}

	if len(hashes) == 1 {
		return hashes[0]
	}

	// Build balanced binary tree from hashes
	currentLevel := make([]Hash, len(hashes))
	copy(currentLevel, hashes)

	for len(currentLevel) > 1 {
		nextLevel := make([]Hash, 0, (len(currentLevel)+1)/2)

		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// Pair: hash the two children
				parent := HashInternal(currentLevel[i], currentLevel[i+1])
				nextLevel = append(nextLevel, parent)
			} else {
				// Odd one out: promote to next level
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		currentLevel = nextLevel
	}

	return currentLevel[0]
}

// StreamingTreeBuilder builds trees incrementally with bounded memory
type StreamingTreeBuilder struct {
	*TreeBuilder

	// Maximum nodes to keep in memory before flushing
	maxMemoryNodes int

	// Current batch being built
	currentBatch []*MerkleNode

	// Completed subtree roots waiting to be merged
	subtreeRoots []*MerkleNode
}

// NewStreamingTreeBuilder creates a streaming builder with memory limits
func NewStreamingTreeBuilder(compareFunc func(a, b []byte) int, maxMemoryNodes int) *StreamingTreeBuilder {
	if maxMemoryNodes <= 0 {
		maxMemoryNodes = 10000 // Default 10k nodes
	}

	return &StreamingTreeBuilder{
		TreeBuilder:    NewTreeBuilder(compareFunc),
		maxMemoryNodes: maxMemoryNodes,
		currentBatch:   make([]*MerkleNode, 0, maxMemoryNodes),
		subtreeRoots:   make([]*MerkleNode, 0, 16),
	}
}

// Add adds a KV pair to the streaming builder
func (stb *StreamingTreeBuilder) Add(key, value []byte, version uint64) error {
	leaf := NewLeafNode(key, value, version)
	stb.currentBatch = append(stb.currentBatch, leaf)

	// Check if we need to flush
	if len(stb.currentBatch) >= stb.maxMemoryNodes {
		return stb.flushBatch()
	}

	return nil
}

// flushBatch builds a subtree from current batch and stores its root
func (stb *StreamingTreeBuilder) flushBatch() error {
	if len(stb.currentBatch) == 0 {
		return nil
	}

	// Build subtree from current batch
	builder := NewTreeBuilder(stb.compare)
	builder.leaves = stb.currentBatch

	root, err := builder.Build()
	if err != nil {
		return err
	}

	// Store subtree root
	stb.subtreeRoots = append(stb.subtreeRoots, root)

	// Reset batch
	stb.currentBatch = make([]*MerkleNode, 0, stb.maxMemoryNodes)

	return nil
}

// Finalize completes the tree building and returns the root
func (stb *StreamingTreeBuilder) Finalize() (*MerkleNode, error) {
	// Flush any remaining batch
	if err := stb.flushBatch(); err != nil {
		return nil, err
	}

	if len(stb.subtreeRoots) == 0 {
		return nil, ErrEmptyTree
	}

	// If only one subtree, return it
	if len(stb.subtreeRoots) == 1 {
		return stb.subtreeRoots[0], nil
	}

	// Merge all subtrees into final tree
	// This is done level by level to maintain balance
	return stb.mergeSubtrees()
}

// mergeSubtrees merges all subtree roots into a single tree
func (stb *StreamingTreeBuilder) mergeSubtrees() (*MerkleNode, error) {
	currentLevel := stb.subtreeRoots

	for len(currentLevel) > 1 {
		nextLevel := make([]*MerkleNode, 0, (len(currentLevel)+1)/2)

		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				parent := NewInternalNode(currentLevel[i], currentLevel[i+1])
				nextLevel = append(nextLevel, parent)
			} else {
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		currentLevel = nextLevel
	}

	return currentLevel[0], nil
}

// GetStats returns statistics about the built tree
type TreeStats struct {
	TotalNodes  int
	TotalLeaves int
	TreeHeight  int
}

func (tb *TreeBuilder) GetStats() TreeStats {
	return TreeStats{
		TotalNodes:  tb.totalNodes,
		TotalLeaves: tb.totalLeaves,
		TreeHeight:  tb.treeHeight,
	}
}

// Utilities for tree operations

// MergeTrees merges two Merkle trees into one
// Both trees must be complete and their keys must be disjoint and ordered
func MergeTrees(left, right *MerkleNode, leftMaxKey, rightMinKey []byte, compare func(a, b []byte) int) (*MerkleNode, error) {
	if left == nil || right == nil {
		return nil, errors.New("mlsm: cannot merge nil trees")
	}

	// Verify ordering constraint
	if compare(leftMaxKey, rightMinKey) >= 0 {
		return nil, errors.New("mlsm: trees overlap, cannot merge")
	}

	// Create new root combining both trees
	root := NewInternalNode(left, right)
	return root, nil
}

// LeafIterator provides iteration over leaf nodes in a tree
type LeafIterator struct {
	root    *MerkleNode
	stack   []*MerkleNode
	current *MerkleNode
}

// NewLeafIterator creates an iterator over leaf nodes
func NewLeafIterator(root *MerkleNode) *LeafIterator {
	iter := &LeafIterator{
		root:  root,
		stack: make([]*MerkleNode, 0, 32),
	}
	iter.pushLeft(root)
	return iter
}

// pushLeft pushes all left children onto the stack
func (it *LeafIterator) pushLeft(node *MerkleNode) {
	for node != nil {
		it.stack = append(it.stack, node)
		if node.IsLeaf() {
			break
		}
		node = node.Left
	}
}

// Next advances to the next leaf
func (it *LeafIterator) Next() bool {
	if len(it.stack) == 0 {
		return false
	}

	// Pop from stack
	node := it.stack[len(it.stack)-1]
	it.stack = it.stack[:len(it.stack)-1]
	it.current = node

	// If internal node, push right subtree
	if node.IsInternal() && node.Right != nil {
		it.pushLeft(node.Right)
	}

	return true
}

// Leaf returns the current leaf node
func (it *LeafIterator) Leaf() *MerkleNode {
	return it.current
}

// CollectLeaves collects all leaves from a tree into a slice
func CollectLeaves(root *MerkleNode) []*MerkleNode {
	if root == nil {
		return nil
	}

	leaves := make([]*MerkleNode, 0)
	iter := NewLeafIterator(root)

	for iter.Next() {
		leaf := iter.Leaf()
		if leaf.IsLeaf() {
			leaves = append(leaves, leaf)
		}
	}

	return leaves
}

// SortAndDeduplicate sorts leaves and removes duplicates, keeping the latest version
func SortAndDeduplicate(leaves []*MerkleNode, compare func(a, b []byte) int) []*MerkleNode {
	if len(leaves) <= 1 {
		return leaves
	}

	// Sort by (key, version) with version descending
	sort.Slice(leaves, func(i, j int) bool {
		cmp := compare(leaves[i].Key, leaves[j].Key)
		if cmp != 0 {
			return cmp < 0
		}
		// Same key, prefer higher version
		return leaves[i].Version > leaves[j].Version
	})

	// Deduplicate: keep first occurrence of each key (highest version)
	result := make([]*MerkleNode, 0, len(leaves))
	result = append(result, leaves[0])

	for i := 1; i < len(leaves); i++ {
		if compare(leaves[i].Key, result[len(result)-1].Key) != 0 {
			result = append(result, leaves[i])
		}
	}

	return result
}
