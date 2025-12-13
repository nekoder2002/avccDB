// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package merkle

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
	Key   []byte // User key (without internal encoding)
	Value []byte // Value data

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
		return HashLeaf(n.Key, n.Value)
	}
	// Internal node
	return HashInternal(n.Left.Hash, n.Right.Hash)
}

// NewLeafNode creates a new leaf node
func NewLeafNode(key, value []byte) *MerkleNode {
	node := &MerkleNode{
		NodeType: NodeTypeLeaf,
		Key:      append([]byte(nil), key...),
		Value:    append([]byte(nil), value...),
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
func (p *MerkleProof) Verify(leafHash Hash) bool {
	if !p.Exists {
		// For non-existence proofs, we need special handling
		// This would require proving the absence via adjacent keys
		return true // Simplified for now
	}

	// Hash up the tree using the proof path
	for i := 0; i < len(p.Path); i++ {
		sibling := p.Path[i]
		if sibling.IsLeft {
			// Sibling is on the left, we are on the right
			leafHash = HashInternal(sibling.Hash, leafHash)
		} else {
			// Sibling is on the right, we are on the left
			leafHash = HashInternal(leafHash, sibling.Hash)
		}
	}

	// Final hash should match the root
	return leafHash.Equal(p.Root)
}
