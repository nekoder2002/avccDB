// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

import (
	"bytes"
	"fmt"
	"testing"
)

// TestHashFunctions tests hash computation functions
func TestHashFunctions(t *testing.T) {
	key := []byte("testkey")
	value := []byte("testvalue")

	// Test leaf hash
	h1 := HashLeaf(key, value)
	if h1.IsZero() {
		t.Fatal("Hash should not be zero")
	}

	// Test versioned hash
	h2 := HashWithVersion(123, key, value)
	if h2.IsZero() {
		t.Fatal("Versioned hash should not be zero")
	}

	// Different versions should produce different hashes
	h3 := HashWithVersion(456, key, value)
	if h2.Equal(h3) {
		t.Fatal("Different versions should produce different hashes")
	}

	// Test internal node hash
	h4 := HashInternal(h1, h2)
	if h4.IsZero() {
		t.Fatal("Internal hash should not be zero")
	}

	t.Logf("Leaf hash: %s", h1.String())
	t.Logf("Versioned hash: %s", h2.String())
	t.Logf("Internal hash: %s", h4.String())
}

// TestMerkleNodeSerialization tests node serialization
func TestMerkleNodeSerialization(t *testing.T) {
	// Test leaf node
	leaf := NewLeafNode([]byte("key1"), []byte("value1"), 100)

	data, err := leaf.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal leaf: %v", err)
	}

	var leaf2 MerkleNode
	if err := leaf2.UnmarshalBinary(data); err != nil {
		t.Fatalf("Failed to unmarshal leaf: %v", err)
	}

	if !bytes.Equal(leaf.Key, leaf2.Key) {
		t.Fatal("Key mismatch after serialization")
	}
	if !bytes.Equal(leaf.Value, leaf2.Value) {
		t.Fatal("Value mismatch after serialization")
	}
	if leaf.Version != leaf2.Version {
		t.Fatal("Version mismatch after serialization")
	}
	if !leaf.Hash.Equal(leaf2.Hash) {
		t.Fatal("Hash mismatch after serialization")
	}

	t.Logf("Serialized size: %d bytes", len(data))
}

// TestTreeBuilder tests basic tree construction
func TestTreeBuilder(t *testing.T) {
	builder := NewTreeBuilder(bytes.Compare)

	// Add sorted leaves
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := builder.AddLeaf(key, value, uint64(i)); err != nil {
			t.Fatalf("Failed to add leaf: %v", err)
		}
	}

	root, err := builder.Build()
	if err != nil {
		t.Fatalf("Failed to build tree: %v", err)
	}

	if root == nil {
		t.Fatal("Root should not be nil")
	}

	stats := builder.GetStats()
	t.Logf("Tree stats: nodes=%d, leaves=%d, height=%d",
		stats.TotalNodes, stats.TotalLeaves, stats.TreeHeight)

	if stats.TotalLeaves != 10 {
		t.Fatalf("Expected 10 leaves, got %d", stats.TotalLeaves)
	}
}

// TestMerkleProof tests proof generation and verification
func TestMerkleProof(t *testing.T) {
	// Build a small tree
	pairs := []KVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Version: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Version: 2},
		{Key: []byte("key3"), Value: []byte("value3"), Version: 3},
		{Key: []byte("key4"), Value: []byte("value4"), Version: 4},
	}

	root, err := BuildFromSorted(pairs, bytes.Compare)
	if err != nil {
		t.Fatalf("Failed to build tree: %v", err)
	}

	tree := NewMerkleTree(root, bytes.Compare)

	// Test proof for existing key
	proof, err := tree.GenerateProof([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to generate proof: %v", err)
	}

	if !proof.Exists {
		t.Fatal("Key should exist")
	}

	if !bytes.Equal(proof.Value, []byte("value2")) {
		t.Fatal("Value mismatch in proof")
	}

	// Verify proof
	if !proof.Verify() {
		t.Fatal("Proof verification failed")
	}

	// Verify against tree root
	if !tree.VerifyProof(proof) {
		t.Fatal("Proof verification against tree failed")
	}

	t.Logf("Proof path length: %d", len(proof.Path))
	t.Logf("Root hash: %s", proof.Root.String())
}

// TestStreamingTreeBuilder tests memory-bounded tree building
func TestStreamingTreeBuilder(t *testing.T) {
	builder := NewStreamingTreeBuilder(bytes.Compare, 100) // Small batch size

	// Add many entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := builder.Add(key, value, uint64(i)); err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}

	root, err := builder.Finalize()
	if err != nil {
		t.Fatalf("Failed to finalize tree: %v", err)
	}

	if root == nil {
		t.Fatal("Root should not be nil")
	}

	tree := NewMerkleTree(root, bytes.Compare)

	// Verify we can get some values
	value, version, found := tree.Get([]byte("key000500"))
	if !found {
		t.Fatal("Key should be found")
	}
	if !bytes.Equal(value, []byte("value000500")) {
		t.Fatal("Value mismatch")
	}
	if version != 500 {
		t.Fatalf("Version mismatch: expected 500, got %d", version)
	}

	t.Logf("Successfully built tree with %d entries", numEntries)
}

// TestCompactTreeFormat tests compact serialization format
func TestCompactTreeFormat(t *testing.T) {
	// Build tree
	pairs := make([]KVPair, 100)
	for i := 0; i < 100; i++ {
		pairs[i] = KVPair{
			Key:     []byte(fmt.Sprintf("key%03d", i)),
			Value:   []byte(fmt.Sprintf("value%03d", i)),
			Version: uint64(i),
		}
	}

	root, err := BuildFromSorted(pairs, bytes.Compare)
	if err != nil {
		t.Fatalf("Failed to build tree: %v", err)
	}

	// Create compact format
	compact := BuildCompactFormat(root)

	// Serialize
	data, err := compact.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal compact format: %v", err)
	}

	// Deserialize
	var compact2 CompactTreeFormat
	if err := compact2.Unmarshal(data); err != nil {
		t.Fatalf("Failed to unmarshal compact format: %v", err)
	}

	// Verify
	if !compact.RootHash.Equal(compact2.RootHash) {
		t.Fatal("Root hash mismatch")
	}
	if compact.Height != compact2.Height {
		t.Fatal("Height mismatch")
	}
	if compact.NumLeaves != compact2.NumLeaves {
		t.Fatalf("Num leaves mismatch: expected %d, got %d",
			compact.NumLeaves, compact2.NumLeaves)
	}

	t.Logf("Compact format size: %d bytes for %d leaves", len(data), compact.NumLeaves)
	t.Logf("Root hash: %s", compact.RootHash.String())
}

// BenchmarkTreeBuild benchmarks tree construction
func BenchmarkTreeBuild(b *testing.B) {
	// Prepare data
	numEntries := 10000
	pairs := make([]KVPair, numEntries)
	for i := 0; i < numEntries; i++ {
		pairs[i] = KVPair{
			Key:     []byte(fmt.Sprintf("key%08d", i)),
			Value:   []byte(fmt.Sprintf("value%08d", i)),
			Version: uint64(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := BuildFromSorted(pairs, bytes.Compare)
		if err != nil {
			b.Fatalf("Failed to build tree: %v", err)
		}
	}
}

// BenchmarkProofGeneration benchmarks proof generation
func BenchmarkProofGeneration(b *testing.B) {
	// Build tree once
	numEntries := 10000
	pairs := make([]KVPair, numEntries)
	for i := 0; i < numEntries; i++ {
		pairs[i] = KVPair{
			Key:     []byte(fmt.Sprintf("key%08d", i)),
			Value:   []byte(fmt.Sprintf("value%08d", i)),
			Version: uint64(i),
		}
	}

	root, err := BuildFromSorted(pairs, bytes.Compare)
	if err != nil {
		b.Fatalf("Failed to build tree: %v", err)
	}

	tree := NewMerkleTree(root, bytes.Compare)
	testKey := []byte("key00005000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := tree.GenerateProof(testKey)
		if err != nil {
			b.Fatalf("Failed to generate proof: %v", err)
		}
	}
}

// BenchmarkProofVerification benchmarks proof verification
func BenchmarkProofVerification(b *testing.B) {
	// Build tree and generate proof once
	numEntries := 10000
	pairs := make([]KVPair, numEntries)
	for i := 0; i < numEntries; i++ {
		pairs[i] = KVPair{
			Key:     []byte(fmt.Sprintf("key%08d", i)),
			Value:   []byte(fmt.Sprintf("value%08d", i)),
			Version: uint64(i),
		}
	}

	root, _ := BuildFromSorted(pairs, bytes.Compare)
	tree := NewMerkleTree(root, bytes.Compare)
	proof, _ := tree.GenerateProof([]byte("key00005000"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !proof.Verify() {
			b.Fatal("Proof verification failed")
		}
	}
}
