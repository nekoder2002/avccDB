// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package dbkey

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrInternalKeyCorrupted records internal key corruption.
type ErrInternalKeyCorrupted struct {
	Ikey   []byte
	Reason string
}

func (e *ErrInternalKeyCorrupted) Error() string {
	return fmt.Sprintf("leveldb: internal key %q corrupted: %s", e.Ikey, e.Reason)
}

func newErrInternalKeyCorrupted(ikey []byte, reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrInternalKeyCorrupted{append([]byte(nil), ikey...), reason})
}

type KeyType uint

func (kt KeyType) String() string {
	switch kt {
	case KeyTypeDel:
		return "d"
	case KeyTypeVal:
		return "v"
	}
	return fmt.Sprintf("<invalid:%#x>", uint(kt))
}

// Value types encoded as the last component of internal keys.
// Don't modify; this value are saved to disk.
const (
	KeyTypeDel = KeyType(0)
	KeyTypeVal = KeyType(1)
)

// KeyTypeSeek defines the KeyType that should be passed when constructing an
// internal key for seeking to a particular sequence number (since we
// sort sequence numbers in decreasing order and the value type is
// embedded as the low 8 bits in the sequence number in internal keys,
// we need to use the highest-numbered ValueType, not the lowest).
const KeyTypeSeek = KeyTypeVal

const (
	// Maximum value possible for sequence number; the 8-bits are
	// used by value type, so its can packed together in single
	// 64-bit integer.
	KeyMaxSeq = (uint64(1) << 56) - 1
	// Maximum value possible for packed sequence number and type.
	KeyMaxNum = (KeyMaxSeq << 8) | uint64(KeyTypeSeek)
	// LatestVersion is used to query the latest version of a key.
	// We use max uint64 so it sorts before all actual versions (since version is descending)
	LastestVersion = ^uint64(0)
)

// Maximum number encoded in bytes.
var KeyMaxNumBytes = make([]byte, 8)

func init() {
	binary.LittleEndian.PutUint64(KeyMaxNumBytes, KeyMaxNum)
}

type InternalKey []byte

func MakeInternalKey(dst, ukey []byte, seq uint64, kt KeyType) InternalKey {
	if seq > KeyMaxSeq {
		panic("leveldb: invalid sequence number")
	} else if kt > KeyTypeVal {
		panic("leveldb: invalid type")
	}

	dst = ensureBuffer(dst, len(ukey)+8)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):], (seq<<8)|uint64(kt))
	return InternalKey(dst)
}

// MakeInternalKeyWithVersion creates an internal key with version support
// Format: uvkey | version (8 bytes) | seq+type (8 bytes)
func MakeInternalKeyWithVersion(dst, ukey []byte, version, seq uint64, kt KeyType) InternalKey {
	if seq > KeyMaxSeq {
		panic("leveldb: invalid sequence number")
	} else if kt > KeyTypeVal {
		panic("leveldb: invalid type")
	}

	dst = ensureBuffer(dst, len(ukey)+16)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):], version)
	binary.LittleEndian.PutUint64(dst[len(ukey)+8:], (seq<<8)|uint64(kt))
	return InternalKey(dst)
}

func ParseInternalKey(ik []byte) (uvkey []byte, seq uint64, kt KeyType, err error) {
	if len(ik) < 8 {
		return nil, 0, 0, newErrInternalKeyCorrupted(ik, "invalid length")
	}
	num := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	seq, kt = num>>8, KeyType(num&0xff)
	if kt > KeyTypeVal {
		return nil, 0, 0, newErrInternalKeyCorrupted(ik, "invalid type")
	}
	uvkey = ik[:len(ik)-8]
	return
}

// ParseInternalKeyWithVersion parses internal key with version
func ParseInternalKeyWithVersion(ik []byte) (ukey []byte, version, seq uint64, kt KeyType, err error) {
	if len(ik) < 16 {
		return nil, 0, 0, 0, newErrInternalKeyCorrupted(ik, "invalid length for versioned key")
	}
	version = binary.LittleEndian.Uint64(ik[len(ik)-16 : len(ik)-8])
	num := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	seq, kt = num>>8, KeyType(num&0xff)
	if kt > KeyTypeVal {
		return nil, 0, 0, 0, newErrInternalKeyCorrupted(ik, "invalid type")
	}
	ukey = ik[:len(ik)-16]
	return
}

//// ExtractVersion extracts version from internal key if present
//func ExtractVersion(ik []byte) uint64 {
//	return binary.LittleEndian.Uint64(ik[len(ik)-16 : len(ik)-8])
//}

// MakeUVKey creates a byte array combining uvkey and version
// Format: uvkey | version (8 bytes)
func MakeUVKey(dst, ukey []byte, version uint64) []byte {
	dst = ensureBuffer(dst, len(ukey)+8)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):], version)
	return dst
}

func ValidInternalKey(ik []byte) bool {
	_, _, _, err := ParseInternalKey(ik)
	return err == nil
}

func (ik InternalKey) assert() {
	if ik == nil {
		panic("leveldb: nil InternalKey")
	}
	if len(ik) < 8 {
		panic(fmt.Sprintf("leveldb: internal key %q, len=%d: invalid length", []byte(ik), len(ik)))
	}
}

func (ik InternalKey) UVkey() []byte {
	ik.assert()
	return ik[:len(ik)-8]
}

func (ik InternalKey) Num() uint64 {
	ik.assert()
	return binary.LittleEndian.Uint64(ik[len(ik)-8:])
}

func (ik InternalKey) ParseNum() (seq uint64, kt KeyType) {
	num := ik.Num()
	seq, kt = num>>8, KeyType(num&0xff)
	if kt > KeyTypeVal {
		panic(fmt.Sprintf("leveldb: internal key %q, len=%d: invalid type %#x", []byte(ik), len(ik), kt))
	}
	return
}

func (ik InternalKey) String() string {
	if ik == nil {
		return "<nil>"
	}

	if uvkey, seq, kt, err := ParseInternalKey(ik); err == nil {
		return fmt.Sprintf("%s,%s%d", shorten(string(uvkey)), kt, seq)
	}
	return fmt.Sprintf("<invalid:%#x>", []byte(ik))
}
