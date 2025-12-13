// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/dbkey"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

type iComparer struct {
	ucmp comparer.Comparer
}

func (icmp *iComparer) uName() string {
	return icmp.ucmp.Name()
}

func (icmp *iComparer) uCompare(a, b []byte) int {
	return icmp.ucmp.Compare(a, b)
}

func (icmp *iComparer) uSeparator(dst, a, b []byte) []byte {
	return icmp.ucmp.Separator(dst, a, b)
}

func (icmp *iComparer) uSuccessor(dst, b []byte) []byte {
	return icmp.ucmp.Successor(dst, b)
}

func (icmp *iComparer) Name() string {
	return icmp.uName()
}

// Compare compares two internal keys with version support.
// Key format: ukey | version (8 bytes) | seq+type (8 bytes)
// Ordering:
//  1. ukey (ascending)
//  2. version (descending) - higher version first
//  3. seq (descending) - higher seq first
func (icmp *iComparer) Compare(a, b []byte) int {
	// Both keys must be versioned (at least 16 bytes for version + seq)
	if len(a) < 16 || len(b) < 16 {
		// Fallback to simple comparison
		return icmp.ucmp.Compare(a, b)
	}

	// Extract ukey (without version and seq)
	ukeyA := a[:len(a)-16]
	ukeyB := b[:len(b)-16]

	// First compare ukey (ascending)
	x := icmp.uCompare(ukeyA, ukeyB)
	if x != 0 {
		return x
	}

	// Same ukey, compare version (descending - higher version first)
	versionA := binary.LittleEndian.Uint64(a[len(a)-16 : len(a)-8])
	versionB := binary.LittleEndian.Uint64(b[len(b)-16 : len(b)-8])
	if versionA > versionB {
		return -1 // Higher version comes first
	} else if versionA < versionB {
		return 1
	}

	// Same version, compare seq (descending - higher seq first)
	seqA := binary.LittleEndian.Uint64(a[len(a)-8:])
	seqB := binary.LittleEndian.Uint64(b[len(b)-8:])
	if seqA > seqB {
		return -1
	} else if seqA < seqB {
		return 1
	}

	return 0
}

func (icmp *iComparer) Separator(dst, a, b []byte) []byte {
	if len(a) < 16 || len(b) < 16 {
		return nil
	}
	ukeyA := a[:len(a)-16]
	ukeyB := b[:len(b)-16]
	dst = icmp.uSeparator(dst, ukeyA, ukeyB)
	if dst != nil && len(dst) < len(ukeyA) && icmp.uCompare(ukeyA, dst) < 0 {
		// Append highest possible version and seq for the separator
		dst = append(dst, dbkey.KeyMaxNumBytes...)  // version = max
		return append(dst, dbkey.KeyMaxNumBytes...) // seq = max
	}
	return nil
}

func (icmp *iComparer) Successor(dst, b []byte) []byte {
	if len(b) < 16 {
		return nil
	}
	ukeyB := b[:len(b)-16]
	dst = icmp.uSuccessor(dst, ukeyB)
	if dst != nil && len(dst) < len(ukeyB) && icmp.uCompare(ukeyB, dst) < 0 {
		// Append highest possible version and seq
		dst = append(dst, dbkey.KeyMaxNumBytes...)
		return append(dst, dbkey.KeyMaxNumBytes...)
	}
	return nil
}
