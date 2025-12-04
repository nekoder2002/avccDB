// Copyright (c) 2024 mLSM Implementation
// Use of this source code is governed by a BSD-style license

package mlsm

import (
	"bufio"
	"encoding/binary"
	"io"
)

const (
	// Magic number for Merkle tree files
	treeMagic = 0x4D4B4C54 // "MKLT"

	// Version of the serialization format
	treeFormatVersion = 1
)

// TreeSerializer handles serialization of Merkle trees
type TreeSerializer struct {
	writer *bufio.Writer
	offset int64
	nodes  map[*MerkleNode]int64 // Map nodes to their offsets
}

// NewTreeSerializer creates a new serializer
func NewTreeSerializer(w io.Writer) *TreeSerializer {
	return &TreeSerializer{
		writer: bufio.NewWriter(w),
		nodes:  make(map[*MerkleNode]int64),
	}
}

// Serialize writes the entire tree to the writer
// Returns the offset of the root node
func (ts *TreeSerializer) Serialize(root *MerkleNode) (int64, error) {
	if root == nil {
		return -1, ErrEmptyTree
	}

	// Write header
	if err := ts.writeHeader(); err != nil {
		return -1, err
	}

	// Serialize tree recursively (post-order: children before parent)
	rootOffset, err := ts.serializeNode(root)
	if err != nil {
		return -1, err
	}

	// Flush writer
	if err := ts.writer.Flush(); err != nil {
		return -1, err
	}

	return rootOffset, nil
}

// writeHeader writes the file header
func (ts *TreeSerializer) writeHeader() error {
	// Magic number
	if err := binary.Write(ts.writer, binary.BigEndian, uint32(treeMagic)); err != nil {
		return err
	}
	ts.offset += 4

	// Version
	if err := binary.Write(ts.writer, binary.BigEndian, uint32(treeFormatVersion)); err != nil {
		return err
	}
	ts.offset += 4

	return nil
}

// serializeNode serializes a single node and its children
func (ts *TreeSerializer) serializeNode(node *MerkleNode) (int64, error) {
	if node == nil {
		return -1, nil
	}

	// Check if already serialized
	if offset, ok := ts.nodes[node]; ok {
		return offset, nil
	}

	// For internal nodes, serialize children first
	if node.IsInternal() {
		leftOffset, err := ts.serializeNode(node.Left)
		if err != nil {
			return -1, err
		}
		node.LeftOffset = leftOffset

		rightOffset, err := ts.serializeNode(node.Right)
		if err != nil {
			return -1, err
		}
		node.RightOffset = rightOffset
	}

	// Serialize this node
	nodeOffset := ts.offset
	data, err := node.MarshalBinary()
	if err != nil {
		return -1, err
	}

	if _, err := ts.writer.Write(data); err != nil {
		return -1, err
	}

	ts.offset += int64(len(data))
	ts.nodes[node] = nodeOffset

	return nodeOffset, nil
}

// TreeDeserializer handles deserialization of Merkle trees
type TreeDeserializer struct {
	reader     io.ReaderAt
	cache      map[int64]*MerkleNode // Cache for loaded nodes
	rootOffset int64
}

// NewTreeDeserializer creates a new deserializer
func NewTreeDeserializer(r io.ReaderAt, rootOffset int64) *TreeDeserializer {
	return &TreeDeserializer{
		reader:     r,
		cache:      make(map[int64]*MerkleNode),
		rootOffset: rootOffset,
	}
}

// LoadRoot loads and returns the root node
func (td *TreeDeserializer) LoadRoot() (*MerkleNode, error) {
	return td.loadNode(td.rootOffset)
}

// loadNode loads a node from the given offset
func (td *TreeDeserializer) loadNode(offset int64) (*MerkleNode, error) {
	if offset < 0 {
		return nil, nil
	}

	// Check cache
	if node, ok := td.cache[offset]; ok {
		return node, nil
	}

	// Read node header to determine size
	headerBuf := make([]byte, nodeHeaderSize)
	if _, err := td.reader.ReadAt(headerBuf, offset); err != nil {
		return nil, err
	}

	node := &MerkleNode{}
	node.NodeType = NodeType(headerBuf[0])

	// Determine full node size
	var nodeSize int
	if node.IsLeaf() {
		// Need to read more to get key/value lengths
		extraBuf := make([]byte, leafNodeExtraSize)
		if _, err := td.reader.ReadAt(extraBuf, offset+nodeHeaderSize); err != nil {
			return nil, err
		}

		keyLen := binary.BigEndian.Uint32(extraBuf[8:12])
		valueLen := binary.BigEndian.Uint32(extraBuf[12:16])
		nodeSize = nodeHeaderSize + leafNodeExtraSize + int(keyLen) + int(valueLen)
	} else {
		nodeSize = nodeHeaderSize + internalNodeExtraSize
	}

	// Read full node
	nodeBuf := make([]byte, nodeSize)
	if _, err := td.reader.ReadAt(nodeBuf, offset); err != nil {
		return nil, err
	}

	// Unmarshal
	if err := node.UnmarshalBinary(nodeBuf); err != nil {
		return nil, err
	}

	// Cache it
	td.cache[offset] = node

	return node, nil
}

// LoadNodeWithChildren loads a node and its immediate children
func (td *TreeDeserializer) LoadNodeWithChildren(offset int64) (*MerkleNode, error) {
	node, err := td.loadNode(offset)
	if err != nil || node == nil {
		return node, err
	}

	if node.IsInternal() {
		// Load children
		node.Left, err = td.loadNode(node.LeftOffset)
		if err != nil {
			return nil, err
		}

		node.Right, err = td.loadNode(node.RightOffset)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// LoadFullTree loads the entire tree into memory
func (td *TreeDeserializer) LoadFullTree() (*MerkleNode, error) {
	root, err := td.loadNode(td.rootOffset)
	if err != nil {
		return nil, err
	}

	if root == nil {
		return nil, ErrEmptyTree
	}

	// Recursively load all children
	return td.loadSubtree(root)
}

// loadSubtree recursively loads all descendants
func (td *TreeDeserializer) loadSubtree(node *MerkleNode) (*MerkleNode, error) {
	if node == nil || node.IsLeaf() {
		return node, nil
	}

	var err error
	node.Left, err = td.loadNode(node.LeftOffset)
	if err != nil {
		return nil, err
	}
	node.Left, err = td.loadSubtree(node.Left)
	if err != nil {
		return nil, err
	}

	node.Right, err = td.loadNode(node.RightOffset)
	if err != nil {
		return nil, err
	}
	node.Right, err = td.loadSubtree(node.Right)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// CompactTreeFormat provides a more efficient format for storing tree metadata
// Instead of storing full tree, store only internal node structure
type CompactTreeFormat struct {
	RootHash  Hash
	Height    int32
	NumLeaves int32

	// Compact representation: array of hashes in level-order
	InternalHashes []Hash
}

// BuildCompactFormat creates a compact representation
func BuildCompactFormat(root *MerkleNode) *CompactTreeFormat {
	if root == nil {
		return nil
	}

	format := &CompactTreeFormat{
		RootHash:       root.Hash,
		Height:         root.Height,
		InternalHashes: make([]Hash, 0),
	}

	// Traverse tree level-by-level and collect internal node hashes
	queue := []*MerkleNode{root}
	leafCount := 0

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if node.IsLeaf() {
			leafCount++
		} else {
			format.InternalHashes = append(format.InternalHashes, node.Hash)
			if node.Left != nil {
				queue = append(queue, node.Left)
			}
			if node.Right != nil {
				queue = append(queue, node.Right)
			}
		}
	}

	format.NumLeaves = int32(leafCount)
	return format
}

// Marshal serializes the compact format
func (ctf *CompactTreeFormat) Marshal() ([]byte, error) {
	size := HashSize + 4 + 4 + 4 + len(ctf.InternalHashes)*HashSize
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

	// Num internal hashes
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
