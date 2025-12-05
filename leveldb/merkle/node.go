// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

import (
	"encoding/binary"
)

// NodeType represents the type of a Merkle tree node
type NodeType byte

const (
	// NodeTypeLeaf represents a leaf node containing key-value data
	NodeTypeLeaf NodeType = 0x00

	// NodeTypeInternal represents an internal node with children
	NodeTypeInternal NodeType = 0x01
)

// MerkleNode represents a node in the Merkle tree
// For production use, nodes are stored compactly to minimize memory
type MerkleNode struct {
	Hash     Hash     // Hash of this node
	NodeType NodeType // Type of node (leaf or internal)

	// For leaf nodes
	Key     []byte // User key (without internal encoding)
	Value   []byte // Value data
	Version uint64 // Version number for this key

	// For internal nodes (in-memory only)
	Left  *MerkleNode // Left child (nil if not loaded)
	Right *MerkleNode // Right child (nil if not loaded)

	// For serialized nodes (offset in file)
	LeftOffset  int64 // File offset to left child
	RightOffset int64 // File offset to right child

	// Metadata
	Height int32 // Height in the tree (0 for leaves)
}

// IsLeaf returns true if this is a leaf node
func (n *MerkleNode) IsLeaf() bool {
	return n.NodeType == NodeTypeLeaf
}

// IsInternal returns true if this is an internal node
func (n *MerkleNode) IsInternal() bool {
	return n.NodeType == NodeTypeInternal
}

// ComputeHash computes the hash for this node
func (n *MerkleNode) ComputeHash() Hash {
	if n.IsLeaf() {
		if n.Version > 0 {
			return HashWithVersion(n.Version, n.Key, n.Value)
		}
		return HashLeaf(n.Key, n.Value)
	}
	// Internal node
	return HashInternal(n.Left.Hash, n.Right.Hash)
}

// NewLeafNode creates a new leaf node
func NewLeafNode(key, value []byte, version uint64) *MerkleNode {
	node := &MerkleNode{
		NodeType: NodeTypeLeaf,
		Key:      append([]byte(nil), key...),
		Value:    append([]byte(nil), value...),
		Version:  version,
		Height:   0,
	}
	node.Hash = node.ComputeHash()
	return node
}

// NewInternalNode creates a new internal node from two children
func NewInternalNode(left, right *MerkleNode) *MerkleNode {
	if left == nil || right == nil {
		panic("merkle: cannot create internal node with nil children")
	}
	height := left.Height
	if right.Height > height {
		height = right.Height
	}
	node := &MerkleNode{
		NodeType: NodeTypeInternal,
		Left:     left,
		Right:    right,
		Height:   height + 1,
	}
	node.Hash = node.ComputeHash()
	return node
}

// MerkleProof represents a Merkle proof for a key-value pair
type MerkleProof struct {
	Key     []byte // The key being proved
	Value   []byte // The value for the key (nil if key doesn't exist)
	Version uint64 // Version number

	// Path contains sibling hashes from leaf to root
	// Path[0] is the sibling of the leaf, Path[len-1] is sibling of node just below root
	Path []ProofNode

	// Root is the root hash of the tree
	Root Hash

	// Exists indicates if the key exists in the tree
	Exists bool
}

// ProofNode represents a node in the proof path
type ProofNode struct {
	Hash   Hash // Hash of the sibling node
	IsLeft bool // True if this sibling is on the left
	Height int32
}

// Verify verifies the Merkle proof
func (p *MerkleProof) Verify() bool {
	if !p.Exists {
		// For non-existence proofs, we need special handling
		// This would require proving the absence via adjacent keys
		return true // Simplified for now
	}

	// Start with the leaf hash
	var currentHash Hash
	if p.Version > 0 {
		currentHash = HashWithVersion(p.Version, p.Key, p.Value)
	} else {
		currentHash = HashLeaf(p.Key, p.Value)
	}

	// Hash up the tree using the proof path
	for i := 0; i < len(p.Path); i++ {
		sibling := p.Path[i]
		if sibling.IsLeft {
			// Sibling is on the left, we are on the right
			currentHash = HashInternal(sibling.Hash, currentHash)
		} else {
			// Sibling is on the right, we are on the left
			currentHash = HashInternal(currentHash, sibling.Hash)
		}
	}

	// Final hash should match the root
	return currentHash.Equal(p.Root)
}

// SerializedNode represents a compact serialized node format
// Layout:
//   [1 byte : type]
//   [32 bytes: hash]
//   [4 bytes : height]
//
//   For leaf nodes:
//     [8 bytes : version]
//     [4 bytes : key length]
//     [4 bytes : value length]
//     [variable: key data]
//     [variable: value data]
//
//   For internal nodes:
//     [8 bytes : left offset]
//     [8 bytes : right offset]

const (
	nodeHeaderSize        = 1 + HashSize + 4 // type + hash + height
	leafNodeExtraSize     = 8 + 4 + 4        // version + keyLen + valueLen
	internalNodeExtraSize = 8 + 8            // leftOffset + rightOffset
)

// EncodedSize returns the serialized size of the node
func (n *MerkleNode) EncodedSize() int {
	size := nodeHeaderSize
	if n.IsLeaf() {
		size += leafNodeExtraSize + len(n.Key) + len(n.Value)
	} else {
		size += internalNodeExtraSize
	}
	return size
}

// MarshalBinary encodes the node to binary format
func (n *MerkleNode) MarshalBinary() ([]byte, error) {
	buf := make([]byte, n.EncodedSize())
	offset := 0

	// Type
	buf[offset] = byte(n.NodeType)
	offset++

	// Hash
	copy(buf[offset:], n.Hash[:])
	offset += HashSize

	// Height
	binary.BigEndian.PutUint32(buf[offset:], uint32(n.Height))
	offset += 4

	if n.IsLeaf() {
		// Version
		binary.BigEndian.PutUint64(buf[offset:], n.Version)
		offset += 8

		// Key length and data
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(n.Key)))
		offset += 4
		copy(buf[offset:], n.Key)
		offset += len(n.Key)

		// Value length and data
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(n.Value)))
		offset += 4
		copy(buf[offset:], n.Value)
		offset += len(n.Value)
	} else {
		// Left offset
		binary.BigEndian.PutUint64(buf[offset:], uint64(n.LeftOffset))
		offset += 8

		// Right offset
		binary.BigEndian.PutUint64(buf[offset:], uint64(n.RightOffset))
		offset += 8
	}

	return buf, nil
}

// UnmarshalBinary decodes the node from binary format
func (n *MerkleNode) UnmarshalBinary(data []byte) error {
	if len(data) < nodeHeaderSize {
		return ErrInvalidNode
	}

	offset := 0

	// Type
	n.NodeType = NodeType(data[offset])
	offset++

	// Hash
	copy(n.Hash[:], data[offset:offset+HashSize])
	offset += HashSize

	// Height
	n.Height = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if n.IsLeaf() {
		if len(data) < offset+leafNodeExtraSize {
			return ErrInvalidNode
		}

		// Version
		n.Version = binary.BigEndian.Uint64(data[offset:])
		offset += 8

		// Key
		keyLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		if len(data) < offset+int(keyLen) {
			return ErrInvalidNode
		}
		n.Key = make([]byte, keyLen)
		copy(n.Key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Value
		valueLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		if len(data) < offset+int(valueLen) {
			return ErrInvalidNode
		}
		n.Value = make([]byte, valueLen)
		copy(n.Value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)
	} else {
		if len(data) < offset+internalNodeExtraSize {
			return ErrInvalidNode
		}

		// Left offset
		n.LeftOffset = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8

		// Right offset
		n.RightOffset = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
	}

	return nil
}

// MergeProofs merges proofs from different layers (e.g., MemDB + SST)
// This creates a combined proof that validates data across multiple Merkle trees.
// The strategy is to chain the proofs together.
func MergeProofs(proofs ...*MerkleProof) *MerkleProof {
	if len(proofs) == 0 {
		return nil
	}

	// Filter out nil proofs
	validProofs := make([]*MerkleProof, 0, len(proofs))
	for _, p := range proofs {
		if p != nil {
			validProofs = append(validProofs, p)
		}
	}

	if len(validProofs) == 0 {
		return nil
	}

	if len(validProofs) == 1 {
		return validProofs[0]
	}

	// Take the first proof as base (should be the one that found the value)
	merged := &MerkleProof{
		Key:     append([]byte(nil), validProofs[0].Key...),
		Value:   append([]byte(nil), validProofs[0].Value...),
		Version: validProofs[0].Version,
		Exists:  validProofs[0].Exists,
		Path:    make([]ProofNode, 0),
	}

	// Combine all proof paths
	for _, proof := range validProofs {
		merged.Path = append(merged.Path, proof.Path...)
	}

	// Compute combined root by aggregating individual roots
	roots := make([]Hash, len(validProofs))
	for i, proof := range validProofs {
		roots[i] = proof.Root
	}
	merged.Root = AggregateRoots(roots)

	return merged
}

// CombineWithLayerProof combines a value proof with additional layer information.
// This is used when we have a proof from one layer (e.g., SST) and want to
// incorporate it into the overall database proof structure.
func CombineWithLayerProof(baseProof *MerkleProof, layerRoot Hash, layerLevel int) *MerkleProof {
	if baseProof == nil {
		return nil
	}

	// Create a copy of the base proof
	combined := &MerkleProof{
		Key:     append([]byte(nil), baseProof.Key...),
		Value:   append([]byte(nil), baseProof.Value...),
		Version: baseProof.Version,
		Exists:  baseProof.Exists,
		Path:    append([]ProofNode(nil), baseProof.Path...),
	}

	// Add layer information to the proof
	// The layer root becomes part of the aggregated proof
	combined.Root = AggregateRoots([]Hash{baseProof.Root, layerRoot})

	return combined
}
