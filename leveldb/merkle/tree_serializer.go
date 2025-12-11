// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

import (
	"encoding/binary"
)

// CompactTreeFormat provides an efficient format for storing tree metadata
// For sorted data, we store leaf hashes and can rebuild the tree structure
type CompactTreeFormat struct {
	RootHash  Hash
	Height    int32
	NumLeaves int32

	// Leaf hashes in order (essential for rebuilding tree)
	LeafHashes []Hash

	// Optional: Store internal node hashes for verification
	// This is much smaller than storing the full tree structure
	InternalHashes []Hash
}

// BuildCompactFormat creates a compact representation
// Collects leaves in-order (left-to-right) to match sorted order
func BuildCompactFormat(root *MerkleNode) *CompactTreeFormat {
	if root == nil {
		return nil
	}

	format := &CompactTreeFormat{
		RootHash:       root.Hash,
		Height:         root.Height,
		LeafHashes:     make([]Hash, 0),
		InternalHashes: make([]Hash, 0),
	}

	// Collect leaves in-order (sorted)
	var collectLeaves func(node *MerkleNode)
	collectLeaves = func(node *MerkleNode) {
		if node == nil {
			return
		}
		if node.IsLeaf() {
			format.LeafHashes = append(format.LeafHashes, node.Hash)
			return
		}
		// In-order traversal for sorted leaves
		collectLeaves(node.Left)
		collectLeaves(node.Right)
	}

	// Collect internal hashes level-by-level
	queue := []*MerkleNode{root}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if node.IsInternal() {
			format.InternalHashes = append(format.InternalHashes, node.Hash)
			if node.Left != nil {
				queue = append(queue, node.Left)
			}
			if node.Right != nil {
				queue = append(queue, node.Right)
			}
		}
	}

	// Collect leaf hashes
	collectLeaves(root)
	format.NumLeaves = int32(len(format.LeafHashes))

	return format
}

// Marshal serializes the compact format
func (ctf *CompactTreeFormat) Marshal() ([]byte, error) {
	// Calculate size: rootHash + height + numLeaves + numLeafHashes + leafHashes + numInternalHashes + internalHashes
	size := HashSize + 4 + 4 + 4 + len(ctf.LeafHashes)*HashSize + 4 + len(ctf.InternalHashes)*HashSize
	buf := make([]byte, size)
	offset := 0

	// Root hash
	copy(buf[offset:], ctf.RootHash[:])
	offset += HashSize

	// Height
	binary.BigEndian.PutUint32(buf[offset:], uint32(ctf.Height))
	offset += 4

	// Num leaves
	binary.BigEndian.PutUint32(buf[offset:], uint32(ctf.NumLeaves))
	offset += 4

	// Num leaf hashes
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(ctf.LeafHashes)))
	offset += 4

	// Leaf hashes
	for _, h := range ctf.LeafHashes {
		copy(buf[offset:], h[:])
		offset += HashSize
	}

	// Num internal hashes
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(ctf.InternalHashes)))
	offset += 4

	// Internal hashes
	for _, h := range ctf.InternalHashes {
		copy(buf[offset:], h[:])
		offset += HashSize
	}

	return buf, nil
}

// Unmarshal deserializes the compact format
func (ctf *CompactTreeFormat) Unmarshal(data []byte) error {
	if len(data) < HashSize+12 {
		return ErrCorruptedData
	}

	offset := 0

	// Root hash
	copy(ctf.RootHash[:], data[offset:offset+HashSize])
	offset += HashSize

	// Height
	ctf.Height = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Num leaves
	ctf.NumLeaves = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Num leaf hashes
	numLeafHashes := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+numLeafHashes*HashSize {
		return ErrCorruptedData
	}

	// Leaf hashes
	ctf.LeafHashes = make([]Hash, numLeafHashes)
	for i := 0; i < numLeafHashes; i++ {
		copy(ctf.LeafHashes[i][:], data[offset:offset+HashSize])
		offset += HashSize
	}

	// Num internal hashes
	if len(data) < offset+4 {
		return ErrCorruptedData
	}
	numHashes := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+numHashes*HashSize {
		return ErrCorruptedData
	}

	// Internal hashes
	ctf.InternalHashes = make([]Hash, numHashes)
	for i := 0; i < numHashes; i++ {
		copy(ctf.InternalHashes[i][:], data[offset:offset+HashSize])
		offset += HashSize
	}

	return nil
}

// GenerateProof generates a Merkle proof directly from CompactTreeFormat
func (ctf *CompactTreeFormat) GenerateProof(leafIndex int) (*MerkleProof, error) {
	if leafIndex < 0 || leafIndex >= len(ctf.LeafHashes) {
		return nil, ErrKeyNotFound
	}

	proof := &MerkleProof{
		Root:   ctf.RootHash,
		Exists: true,
		Path:   make([]ProofNode, 0),
	}

	// Build levels on-the-fly and generate proof
	currentLevel := ctf.LeafHashes
	currentIdx := leafIndex
	level := int32(0)

	for len(currentLevel) > 1 {
		// Find sibling
		isLeft := (currentIdx % 2) == 0
		var siblingIdx int
		if isLeft {
			siblingIdx = currentIdx + 1
		} else {
			siblingIdx = currentIdx - 1
		}

		// Add sibling to proof if it exists
		if siblingIdx >= 0 && siblingIdx < len(currentLevel) {
			proof.Path = append(proof.Path, ProofNode{
				Hash:   currentLevel[siblingIdx],
				IsLeft: !isLeft,
				Height: level,
			})
		}

		// Build next level
		nextLevel := make([]Hash, 0, (len(currentLevel)+1)/2)
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				parent := HashInternal(currentLevel[i], currentLevel[i+1])
				nextLevel = append(nextLevel, parent)
			} else {
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}

		currentLevel = nextLevel
		currentIdx = currentIdx / 2
		level++
	}

	return proof, nil
}

// GenerateProofByHash generates a Merkle proof for a leaf by its hash
// This method finds the leaf hash in LeafHashes and generates the proof
func (ctf *CompactTreeFormat) GenerateProofByHash(leafHash Hash) (*MerkleProof, error) {
	// Find the index of the leaf hash
	leafIndex := -1
	for i, h := range ctf.LeafHashes {
		if h.Equal(leafHash) {
			leafIndex = i
			break
		}
	}

	if leafIndex == -1 {
		return nil, ErrKeyNotFound
	}

	// Use the existing GenerateProof method with the found index
	return ctf.GenerateProof(leafIndex)
}

// GetRoot returns the root hash
func (ctf *CompactTreeFormat) GetRoot() Hash {
	return ctf.RootHash
}

// VerifyProof verifies a Merkle proof
func (ctf *CompactTreeFormat) VerifyProof(proof *MerkleProof, leafHash Hash) bool {
	if proof == nil {
		return false
	}

	currentHash := leafHash
	for _, sibling := range proof.Path {
		if sibling.IsLeft {
			currentHash = HashInternal(sibling.Hash, currentHash)
		} else {
			currentHash = HashInternal(currentHash, sibling.Hash)
		}
	}

	return currentHash.Equal(ctf.RootHash)
}
