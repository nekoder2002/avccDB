// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/dbkey"

	"github.com/syndtr/goleveldb/leveldb/merkle"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// DB is a LevelDB database.
type DB struct {
	// Need 64-bit alignment.
	seq uint64

	// Stats. Need 64-bit alignment.
	cWriteDelay            int64 // The cumulative duration of write delays
	cWriteDelayN           int32 // The cumulative number of write delays
	inWritePaused          int32 // The indicator whether write operation is paused by compaction
	aliveSnaps, aliveIters int32

	// Compaction statistic
	memComp       uint32 // The cumulative number of memory compaction
	level0Comp    uint32 // The cumulative number of level0 compaction
	nonLevel0Comp uint32 // The cumulative number of non-level0 compaction
	seekComp      uint32 // The cumulative number of seek compaction

	// Session.
	s *session

	// MemDB.
	memMu           sync.RWMutex
	memPool         chan *memdb.DB
	mem, frozenMem  *memDB
	journal         *journal.Writer
	journalWriter   storage.Writer
	journalFd       storage.FileDesc
	frozenJournalFd storage.FileDesc
	frozenSeq       uint64

	// Snapshot.
	snapsMu   sync.Mutex
	snapsList *list.List

	// Write.
	batchPool    sync.Pool
	writeMergeC  chan writeMerge
	writeMergedC chan bool
	writeLockC   chan struct{}
	writeAckC    chan error
	writeDelay   time.Duration
	writeDelayN  int
	tr           *Transaction

	// Compaction.
	compCommitLk     sync.Mutex
	tcompCmdC        chan cCmd
	tcompPauseC      chan chan<- struct{}
	mcompCmdC        chan cCmd
	compErrC         chan error
	compPerErrC      chan error
	compErrSetC      chan error
	compWriteLocking bool
	compStats        cStats
	memdbMaxLevel    int // For testing.

	//// mLSM MasterRoot: aggregates Merkle Roots from all levels
	//masterRootMu sync.RWMutex
	//masterRoot   merkle.Hash // Aggregated root hash of all levels

	// Close.
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
	closer io.Closer
}

func openDB(s *session) (*DB, error) {
	s.log("db@open opening")
	start := time.Now()
	db := &DB{
		s: s,
		// Initial sequence
		seq: s.stSeqNum,
		// MemDB
		memPool: make(chan *memdb.DB, 1),
		// Snapshot
		snapsList: list.New(),
		// Write
		batchPool:    sync.Pool{New: newBatch},
		writeMergeC:  make(chan writeMerge),
		writeMergedC: make(chan bool),
		writeLockC:   make(chan struct{}, 1),
		writeAckC:    make(chan error),
		// Compaction
		tcompCmdC:   make(chan cCmd),
		tcompPauseC: make(chan chan<- struct{}),
		mcompCmdC:   make(chan cCmd),
		compErrC:    make(chan error),
		compPerErrC: make(chan error),
		compErrSetC: make(chan error),
		// Close
		closeC: make(chan struct{}),
	}

	// Read-only mode.
	readOnly := s.o.GetReadOnly()

	if readOnly {
		// Recover journals (read-only mode).
		if err := db.recoverJournalRO(); err != nil {
			return nil, err
		}
	} else {
		// Recover journals.
		if err := db.recoverJournal(); err != nil {
			return nil, err
		}

		// Remove any obsolete files.
		if err := db.checkAndCleanFiles(); err != nil {
			// Close journal.
			if db.journal != nil {
				db.journal.Close()
				db.journalWriter.Close()
			}
			return nil, err
		}
	}

	// Doesn't need to be included in the wait group.
	go db.compactionError()
	go db.mpoolDrain()

	if readOnly {
		if err := db.SetReadOnly(); err != nil {
			return nil, err
		}
	} else {
		db.closeW.Add(2)
		go db.tCompaction()
		go db.mCompaction()
		// go db.jWriter()
	}

	//// Initialize MasterRoot after opening
	//db.updateMasterRoot()

	s.logf("db@open done T·%v", time.Since(start))

	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// Open opens or creates a DB for the given storage.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist Open will returns
// os.ErrExist error.
//
// Open will return an error with type of ErrCorrupted if corruption
// detected in the DB. Use errors.IsCorrupted to test whether an error is
// due to corruption. Corrupted DB can be recovered with Recover function.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func Open(stor storage.Storage, o *opt.Options) (db *DB, err error) {
	s, err := newSession(stor, o)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			s.close()
			s.release()
		}
	}()

	err = s.recover()
	if err != nil {
		if !os.IsNotExist(err) || s.o.GetErrorIfMissing() || s.o.GetReadOnly() {
			return
		}
		err = s.create()
		if err != nil {
			return
		}
	} else if s.o.GetErrorIfExist() {
		err = os.ErrExist
		return
	}

	return openDB(s)
}

// OpenFile opens or creates a DB for the given path.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist OpenFile will returns
// os.ErrExist error.
//
// OpenFile uses standard file-system backed storage implementation as
// described in the leveldb/storage package.
//
// OpenFile will return an error with type of ErrCorrupted if corruption
// detected in the DB. Use errors.IsCorrupted to test whether an error is
// due to corruption. Corrupted DB can be recovered with Recover function.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func OpenFile(path string, o *opt.Options) (db *DB, err error) {
	stor, err := storage.OpenFile(path, o.GetReadOnly())
	if err != nil {
		return
	}
	db, err = Open(stor, o)
	if err != nil {
		stor.Close()
	} else {
		db.closer = stor
	}
	return
}

// Recover recovers and opens a DB with missing or corrupted manifest files
// for the given storage. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also, Recover will ignore ErrorIfMissing and ErrorIfExist options.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func Recover(stor storage.Storage, o *opt.Options) (db *DB, err error) {
	s, err := newSession(stor, o)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			s.close()
			s.release()
		}
	}()

	err = recoverTable(s, o)
	if err != nil {
		return
	}
	return openDB(s)
}

// RecoverFile recovers and opens a DB with missing or corrupted manifest files
// for the given path. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also, Recover will ignore ErrorIfMissing and ErrorIfExist options.
//
// RecoverFile uses standard file-system backed storage implementation as described
// in the leveldb/storage package.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func RecoverFile(path string, o *opt.Options) (db *DB, err error) {
	stor, err := storage.OpenFile(path, false)
	if err != nil {
		return
	}
	db, err = Recover(stor, o)
	if err != nil {
		stor.Close()
	} else {
		db.closer = stor
	}
	return
}

func recoverTable(s *session, o *opt.Options) error {
	o = dupOptions(o)
	// Mask StrictReader, lets StrictRecovery doing its job.
	o.Strict &= ^opt.StrictReader

	// Get all tables and sort it by file number.
	fds, err := s.stor.List(storage.TypeTable)
	if err != nil {
		return err
	}
	sortFds(fds)

	var (
		maxSeq                                                            uint64
		recoveredKey, goodKey, corruptedKey, corruptedBlock, droppedTable int

		// We will drop corrupted table.
		strict = o.GetStrict(opt.StrictRecovery)
		noSync = o.GetNoSync()

		rec   = &sessionRecord{}
		bpool = util.NewBufferPool(o.GetBlockSize() + 5)
	)
	buildTable := func(iter iterator.Iterator) (tmpFd storage.FileDesc, size int64, err error) {
		tmpFd = s.newTemp()
		writer, err := s.stor.Create(tmpFd)
		if err != nil {
			return
		}
		defer func() {
			if cerr := writer.Close(); cerr != nil {
				if err == nil {
					err = cerr
				} else {
					err = fmt.Errorf("error recovering table (%v); error closing (%v)", err, cerr)
				}
			}
			if err != nil {
				if rerr := s.stor.Remove(tmpFd); rerr != nil {
					err = fmt.Errorf("error recovering table (%v); error removing (%v)", err, rerr)
				}
				tmpFd = storage.FileDesc{}
			}
		}()

		// Copy entries.
		tw := table.NewWriter(writer, o, nil, 0)
		for iter.Next() {
			key := iter.Key()
			if dbkey.ValidInternalKey(key) {
				err = tw.Append(key, iter.Value())
				if err != nil {
					return
				}
			}
		}
		err = iter.Error()
		if err != nil && !errors.IsCorrupted(err) {
			return
		}
		err = tw.Close()
		if err != nil {
			return
		}
		if !noSync {
			err = writer.Sync()
			if err != nil {
				return
			}
		}
		size = int64(tw.BytesLen())
		return
	}
	recoverTable := func(fd storage.FileDesc) error {
		s.logf("table@recovery recovering @%d", fd.Num)
		reader, err := s.stor.Open(fd)
		if err != nil {
			return err
		}
		var closed bool
		defer func() {
			if !closed {
				reader.Close()
			}
		}()

		// Get file size.
		size, err := reader.Seek(0, 2)
		if err != nil {
			return err
		}

		var (
			tSeq                                     uint64
			tgoodKey, tcorruptedKey, tcorruptedBlock int
			imin, imax                               []byte
		)
		tr, err := table.NewReader(reader, size, fd, nil, bpool, o)
		if err != nil {
			return err
		}
		iter := tr.NewIterator(nil, nil)
		if itererr, ok := iter.(iterator.ErrorCallbackSetter); ok {
			itererr.SetErrorCallback(func(err error) {
				if errors.IsCorrupted(err) {
					s.logf("table@recovery block corruption @%d %q", fd.Num, err)
					tcorruptedBlock++
				}
			})
		}

		// Scan the table.
		for iter.Next() {
			key := iter.Key()
			// Try versioned key first (default), fall back to non-versioned for recovery
			_, _, seq, _, kerr := dbkey.ParseInternalKeyWithVersion(key)
			if kerr != nil {
				// Fall back to non-versioned key for recovery compatibility
				_, seq, _, kerr = dbkey.ParseInternalKey(key)
				if kerr != nil {
					tcorruptedKey++
					continue
				}
			}
			tgoodKey++
			if seq > tSeq {
				tSeq = seq
			}
			if imin == nil {
				imin = append([]byte(nil), key...)
			}
			imax = append(imax[:0], key...)
		}
		if err := iter.Error(); err != nil && !errors.IsCorrupted(err) {
			iter.Release()
			return err
		}
		iter.Release()

		goodKey += tgoodKey
		corruptedKey += tcorruptedKey
		corruptedBlock += tcorruptedBlock

		if strict && (tcorruptedKey > 0 || tcorruptedBlock > 0) {
			droppedTable++
			s.logf("table@recovery dropped @%d Gk·%d Ck·%d Cb·%d S·%d Q·%d", fd.Num, tgoodKey, tcorruptedKey, tcorruptedBlock, size, tSeq)
			return nil
		}

		if tgoodKey > 0 {
			if tcorruptedKey > 0 || tcorruptedBlock > 0 {
				// Rebuild the table.
				s.logf("table@recovery rebuilding @%d", fd.Num)
				iter := tr.NewIterator(nil, nil)
				tmpFd, newSize, err := buildTable(iter)
				iter.Release()
				if err != nil {
					return err
				}
				closed = true
				reader.Close()
				if err := s.stor.Rename(tmpFd, fd); err != nil {
					return err
				}
				size = newSize
			}
			if tSeq > maxSeq {
				maxSeq = tSeq
			}
			recoveredKey += tgoodKey
			// Add table to level 0.
			rec.addTable(0, fd.Num, size, imin, imax)
			s.logf("table@recovery recovered @%d Gk·%d Ck·%d Cb·%d S·%d Q·%d", fd.Num, tgoodKey, tcorruptedKey, tcorruptedBlock, size, tSeq)
		} else {
			droppedTable++
			s.logf("table@recovery unrecoverable @%d Ck·%d Cb·%d S·%d", fd.Num, tcorruptedKey, tcorruptedBlock, size)
		}

		return nil
	}

	// Recover all tables.
	if len(fds) > 0 {
		s.logf("table@recovery F·%d", len(fds))

		// Mark file number as used.
		s.markFileNum(fds[len(fds)-1].Num)

		for _, fd := range fds {
			if err := recoverTable(fd); err != nil {
				return err
			}
		}

		s.logf("table@recovery recovered F·%d N·%d Gk·%d Ck·%d Q·%d", len(fds), recoveredKey, goodKey, corruptedKey, maxSeq)
	}

	// Set sequence number.
	rec.setSeqNum(maxSeq)

	// Create new manifest.
	if err := s.create(); err != nil {
		return err
	}

	// Commit.
	return s.commit(rec, false)
}

func (db *DB) recoverJournal() error {
	// Get all journals and sort it by file number.
	rawFds, err := db.s.stor.List(storage.TypeJournal)
	if err != nil {
		return err
	}
	sortFds(rawFds)

	// Journals that will be recovered.
	var fds []storage.FileDesc
	for _, fd := range rawFds {
		if fd.Num >= db.s.stJournalNum || fd.Num == db.s.stPrevJournalNum {
			fds = append(fds, fd)
		}
	}

	var (
		ofd storage.FileDesc // Obsolete file.
		rec = &sessionRecord{}
	)

	// Recover journals.
	if len(fds) > 0 {
		db.logf("journal@recovery F·%d", len(fds))

		// Mark file number as used.
		db.s.markFileNum(fds[len(fds)-1].Num)

		var (
			// Options.
			strict      = db.s.o.GetStrict(opt.StrictJournal)
			checksum    = db.s.o.GetStrict(opt.StrictJournalChecksum)
			writeBuffer = db.s.o.GetWriteBuffer()

			jr       *journal.Reader
			mdb      = memdb.New(db.s.icmp, writeBuffer)
			buf      = &util.Buffer{}
			batchSeq uint64
			batchLen int
		)

		for _, fd := range fds {
			db.logf("journal@recovery recovering @%d", fd.Num)

			fr, err := db.s.stor.Open(fd)
			if err != nil {
				return err
			}

			// Create or reset journal reader instance.
			if jr == nil {
				jr = journal.NewReader(fr, dropper{db.s, fd}, strict, checksum)
			} else {
				// Ignore the error here
				_ = jr.Reset(fr, dropper{db.s, fd}, strict, checksum)
			}

			// Flush memdb and remove obsolete journal file.
			if !ofd.Zero() {
				if mdb.Len() > 0 {
					if _, err := db.s.flushMemdb(rec, mdb, 0); err != nil {
						fr.Close()
						return err
					}
				}

				rec.setJournalNum(fd.Num)
				rec.setSeqNum(db.seq)
				if err := db.s.commit(rec, false); err != nil {
					fr.Close()
					return err
				}
				rec.resetAddedTables()

				if err := db.s.stor.Remove(ofd); err != nil {
					fr.Close()
					return err
				}
				ofd = storage.FileDesc{}
			}

			// Replay journal to memdb.
			mdb.Reset()
			for {
				r, err := jr.Next()
				if err != nil {
					if err == io.EOF {
						break
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}

				buf.Reset()
				if _, err := buf.ReadFrom(r); err != nil {
					if err == io.ErrUnexpectedEOF {
						// This is error returned due to corruption, with strict == false.
						continue
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}
				batchSeq, batchLen, err = decodeBatchToMem(buf.Bytes(), db.seq, mdb)
				if err != nil {
					if !strict && errors.IsCorrupted(err) {
						db.s.logf("journal error: %v (skipped)", err)
						// We won't apply sequence number as it might be corrupted.
						continue
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}

				// Save sequence number.
				db.seq = batchSeq + uint64(batchLen)

				// Flush it if large enough.
				if mdb.Size() >= writeBuffer {
					if _, err := db.s.flushMemdb(rec, mdb, 0); err != nil {
						fr.Close()
						return err
					}

					mdb.Reset()
				}
			}

			fr.Close()
			ofd = fd
		}

		// Flush the last memdb.
		if mdb.Len() > 0 {
			if _, err := db.s.flushMemdb(rec, mdb, 0); err != nil {
				return err
			}
		}
	}

	// Create a new journal.
	if _, err := db.newMem(0); err != nil {
		return err
	}

	// Commit.
	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.seq)
	if err := db.s.commit(rec, false); err != nil {
		// Close journal on error.
		if db.journal != nil {
			db.journal.Close()
			db.journalWriter.Close()
		}
		return err
	}

	// Remove the last obsolete journal file.
	if !ofd.Zero() {
		if err := db.s.stor.Remove(ofd); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) recoverJournalRO() error {
	// Get all journals and sort it by file number.
	rawFds, err := db.s.stor.List(storage.TypeJournal)
	if err != nil {
		return err
	}
	sortFds(rawFds)

	// Journals that will be recovered.
	var fds []storage.FileDesc
	for _, fd := range rawFds {
		if fd.Num >= db.s.stJournalNum || fd.Num == db.s.stPrevJournalNum {
			fds = append(fds, fd)
		}
	}

	var (
		// Options.
		strict      = db.s.o.GetStrict(opt.StrictJournal)
		checksum    = db.s.o.GetStrict(opt.StrictJournalChecksum)
		writeBuffer = db.s.o.GetWriteBuffer()

		mdb = memdb.New(db.s.icmp, writeBuffer)
	)

	// Recover journals.
	if len(fds) > 0 {
		db.logf("journal@recovery RO·Mode F·%d", len(fds))

		var (
			jr       *journal.Reader
			buf      = &util.Buffer{}
			batchSeq uint64
			batchLen int
		)

		for _, fd := range fds {
			db.logf("journal@recovery recovering @%d", fd.Num)

			fr, err := db.s.stor.Open(fd)
			if err != nil {
				return err
			}

			// Create or reset journal reader instance.
			if jr == nil {
				jr = journal.NewReader(fr, dropper{db.s, fd}, strict, checksum)
			} else {
				if err := jr.Reset(fr, dropper{db.s, fd}, strict, checksum); err != nil {
					return err
				}
			}

			// Replay journal to memdb.
			for {
				r, err := jr.Next()
				if err != nil {
					if err == io.EOF {
						break
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}

				buf.Reset()
				if _, err := buf.ReadFrom(r); err != nil {
					if err == io.ErrUnexpectedEOF {
						// This is error returned due to corruption, with strict == false.
						continue
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}
				batchSeq, batchLen, err = decodeBatchToMem(buf.Bytes(), db.seq, mdb)
				if err != nil {
					if !strict && errors.IsCorrupted(err) {
						db.s.logf("journal error: %v (skipped)", err)
						// We won't apply sequence number as it might be corrupted.
						continue
					}

					fr.Close()
					return errors.SetFd(err, fd)
				}

				// Save sequence number.
				db.seq = batchSeq + uint64(batchLen)
			}

			fr.Close()
		}
	}

	// Set memDB.
	db.mem = &memDB{db: db, DB: mdb, ref: 1}

	return nil
}

func memGet(mdb *memdb.DB, ikey dbkey.InternalKey, icmp *iComparer) (ok bool, mv []byte, err error) {
	mk, mv, err := mdb.Find(ikey)
	if err == nil {
		// Try to parse as versioned key first
		if len(mk) >= 16 {
			ukey, _, _, kt, kerr := dbkey.ParseInternalKeyWithVersion(mk)
			if kerr == nil {
				// Parse query key to get its ukey
				qUkey, _, _, _, qErr := dbkey.ParseInternalKeyWithVersion(ikey)
				if qErr == nil && icmp.uCompare(ukey, qUkey) == 0 {
					if kt == dbkey.KeyTypeDel {
						return true, nil, ErrNotFound
					}
					return true, mv, nil
				}
			}
		}
		// Fallback to non-versioned key parsing
		uvkey, _, kt, kerr := dbkey.ParseInternalKey(mk)
		if kerr != nil {
			// Shouldn't have had happen.
			panic(kerr)
		}
		if icmp.uCompare(uvkey, ikey.UVkey()) == 0 {
			if kt == dbkey.KeyTypeDel {
				return true, nil, ErrNotFound
			}
			return true, mv, nil
		}
	} else if err != ErrNotFound {
		return true, nil, err
	}
	return
}

func (db *DB) get(auxm *memdb.DB, auxt tFiles, key []byte, version, seq uint64, ro *opt.ReadOptions) (value []byte, err error) {
	ikey := dbkey.MakeInternalKeyWithVersion(nil, key, version, seq, dbkey.KeyTypeSeek)
	if auxm != nil {
		if ok, mv, me := memGet(auxm, ikey, db.s.icmp); ok {
			return append([]byte(nil), mv...), me
		}
	}

	em, fm := db.getMems()
	for _, m := range [...]*memDB{em, fm} {
		if m == nil {
			continue
		}
		defer m.decref()

		if ok, mv, me := memGet(m.DB, ikey, db.s.icmp); ok {
			return append([]byte(nil), mv...), me
		}
	}

	v := db.s.version()
	value, cSched, err := v.get(auxt, ikey, ro, false)
	v.release()
	if cSched {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	return
}

// getWithProof gets value and Merkle proof for a key at specified version across all layers
// If version is 0, it searches for the latest version
// Returns: value, actualVersion, proof, error
func (db *DB) getWithProof(auxm *memdb.DB, auxt tFiles, key []byte, version, seq uint64, ro *opt.ReadOptions) (value []byte, actualVersion uint64, proof *DBProof, err error) {
	ikey := dbkey.MakeInternalKeyWithVersion(nil, key, version, seq, dbkey.KeyTypeSeek)

	// Try auxiliary memdb first
	if auxm != nil {
		if ok, _, me := memGet(auxm, ikey, db.s.icmp); ok {
			if me != nil {
				return nil, 0, nil, me
			}
			// Generate MemDB proof
			value, memProof, memRoot, _ := auxm.GetWithProof(ikey)
			actualVersion = version
			if memProof != nil {
				v := db.s.version()
				proof = db.buildMemDBProof(memProof, memRoot, v)
				v.release()
			}
			return append([]byte(nil), value...), actualVersion, proof, nil
		}
	}

	// Try effective and frozen memdb
	em, fm := db.getMems()
	memDBs := [...]*memDB{em, fm}
	for _, m := range memDBs {
		if m == nil {
			continue
		}
		defer m.decref()

		if ok, mv, me := memGet(m.DB, ikey, db.s.icmp); ok {
			if me != nil {
				return nil, 0, nil, me
			}
			// Generate MemDB proof
			_, memProof, memRoot, _ := m.DB.GetWithProof(ikey)
			actualVersion = version

			// Get version from the found value if querying latest
			if version == dbkey.LastestVersion {
				// Try to extract actual version by iterating
				actualVersion = db.ExtractVersionFromMemDB(m.DB, key)
			}

			if memProof != nil {
				v := db.s.version()
				proof = db.buildMemDBProof(memProof, memRoot, v)
				v.release()
			}
			return append([]byte(nil), mv...), actualVersion, proof, nil
		}
	}

	// Try SST files
	v := db.s.version()
	defer v.release()
	value, actualVersion, sstProof, layerProof, cSched, err := v.getWithProof(auxt, ikey, ro)
	if cSched {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}

	if err != nil {
		return nil, 0, nil, err
	}
	// Combine SST proof with layer proof and MasterRoot
	if sstProof != nil && layerProof != nil {
		// Generate master proof for the layer
		masterProof, err := db.generateMasterProof(v, layerProof.Root)
		if err != nil {
			// Log error but don't fail the query
			db.logf("generate master proof error: %v", err)
		}
		proof = &DBProof{
			DataProof:   sstProof,
			LayerProof:  layerProof,
			MasterProof: masterProof,
		}
	}
	return value, actualVersion, proof, nil
}

// buildMemDBProof builds a complete proof for data found in MemDB
func (db *DB) buildMemDBProof(memProof *merkle.MerkleProof, memRoot merkle.Hash, v *version) *DBProof {
	// Get all layer roots for master proof
	// Layers: [MemDB root, Level0 root, Level1 root, ...]
	layerRoots := []merkle.Hash{memRoot}

	if v != nil {
		for level, tables := range v.levels {
			if len(tables) == 0 {
				continue
			}
			// Collect all SST roots in this level
			var sstRoots []merkle.Hash
			for _, t := range tables {
				if root, err := db.s.tops.getMerkleRoot(t); err == nil {
					sstRoots = append(sstRoots, root)
				}
			}
			if len(sstRoots) > 0 {
				levelRoot := merkle.BuildTreeFromHashes(sstRoots)
				layerRoots = append(layerRoots, levelRoot)
			}
			_ = level // Suppress unused warning
		}
	}

	// Build layer proof: MemDB root is at index 0
	var layerProof *merkle.MerkleProof
	var masterProof *merkle.MerkleProof

	if len(layerRoots) > 1 {
		// Build tree from layer roots
		layerTree := merkle.NewMerkleTree(layerRoots)
		// MemDB is at index 0
		layerProof, _ = layerTree.GenerateProof(0)

		// Master proof is trivial when we have the master root
		masterProof = &merkle.MerkleProof{
			Root:   layerTree.GetRoot(),
			Exists: true,
			Path:   nil, // Empty path, root is the master
		}
	} else {
		// Only MemDB, it is the master
		layerProof = &merkle.MerkleProof{
			Root:   memRoot,
			Exists: true,
			Path:   nil,
		}
		masterProof = layerProof
	}

	return &DBProof{
		DataProof:   memProof,
		LayerProof:  layerProof,
		MasterProof: masterProof,
	}
}

// ExtractVersionFromMemDB extracts the actual version for a key from MemDB
func (db *DB) ExtractVersionFromMemDB(mdb *memdb.DB, key []byte) uint64 {
	iter := mdb.NewIterator(nil)
	defer iter.Release()

	seekKey := dbkey.MakeInternalKeyWithVersion(nil, key, dbkey.LastestVersion, dbkey.KeyMaxSeq, dbkey.KeyTypeSeek)
	if iter.Seek(seekKey) {
		ikey := iter.Key()
		ukey, version, _, _, err := dbkey.ParseInternalKeyWithVersion(ikey)
		if err == nil && db.s.icmp.uCompare(ukey, key) == 0 {
			return version
		}
	}
	return 0
}

// getVersionHistory gets all versions of a key within a version range
func (db *DB) getVersionHistory(auxm *memdb.DB, auxt tFiles, key []byte, minVersion, maxVersion uint64, seq uint64, ro *opt.ReadOptions, withProof bool) (entries []VersionEntry, err error) {
	// Collect all matching versions from MemDB and SST files
	// versionMap stores version -> (value, source) where source indicates MemDB or SST
	type versionInfo struct {
		value       []byte
		fromMem     bool
		memDB       *memdb.DB
		internalKey []byte // Store the internal key for MemDB entries
	}
	var versionMap = make(map[uint64]*versionInfo)

	// Helper to collect from MemDB with internal key storage
	collectFromMemDB := func(mdb *memdb.DB) {
		iter := mdb.NewIterator(nil)
		defer iter.Release()

		seekKey := dbkey.MakeInternalKeyWithVersion(nil, key, dbkey.LastestVersion, seq, dbkey.KeyTypeSeek)
		if !iter.Seek(seekKey) {
			return
		}

		for ; iter.Valid(); iter.Next() {
			ikey := iter.Key()
			ukey, version, _, kt, kerr := dbkey.ParseInternalKeyWithVersion(ikey)
			if kerr != nil {
				break
			}
			if db.s.icmp.uCompare(ukey, key) != 0 {
				break
			}
			if minVersion > 0 && version < minVersion {
				continue
			}
			if maxVersion > 0 && version > maxVersion {
				continue
			}
			if kt == dbkey.KeyTypeDel {
				continue
			}
			if _, exists := versionMap[version]; !exists {
				versionMap[version] = &versionInfo{
					value:       append([]byte(nil), iter.Value()...),
					fromMem:     true,
					memDB:       mdb,
					internalKey: append([]byte(nil), ikey...),
				}
			}
		}
	}

	// Search in auxiliary memdb
	if auxm != nil {
		collectFromMemDB(auxm)
	}

	// Search in effective and frozen memdb
	em, fm := db.getMems()
	for _, m := range [...]*memDB{em, fm} {
		if m == nil {
			continue
		}
		defer m.decref()
		collectFromMemDB(m.DB)
	}

	// Search in SST files
	v := db.s.version()
	defer v.release()

	sstEntries, cSched, err := v.getVersionHistory(auxt, key, minVersion, maxVersion, ro)
	if cSched {
		db.compTrigger(db.tcompCmdC)
	}

	if err != nil && err != ErrNotFound {
		return nil, err
	}

	// Merge SST entries into version map
	for _, entry := range sstEntries {
		if _, exists := versionMap[entry.Version]; !exists {
			versionMap[entry.Version] = &versionInfo{
				value:   entry.Value,
				fromMem: false,
			}
		}
	}

	// Convert map to sorted slice
	if len(versionMap) == 0 {
		return nil, ErrNotFound
	}

	entries = make([]VersionEntry, 0, len(versionMap))
	for version, info := range versionMap {
		entry := VersionEntry{
			Version: version,
			Value:   append([]byte(nil), info.value...), // Copy value
		}

		// Get proof if requested
		if withProof {
			if info.fromMem && info.memDB != nil {
				// Generate proof directly from MemDB using the exact internal key
				_, memProof, memRoot, _ := info.memDB.GetWithProof(info.internalKey)
				if memProof != nil {
					entry.Proof = db.buildMemDBProof(memProof, memRoot, v)
				}
			} else {
				// Get proof from SST
				_, actualVersion, proof, proofErr := db.getWithProof(auxm, auxt, key, version, seq, ro)
				if proofErr == nil && actualVersion == version {
					entry.Proof = proof
				}
			}
		}

		entries = append(entries, entry)
	}

	// Sort by version (ascending)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Version < entries[j].Version
	})

	return entries, nil
}

// collectVersionsFromMemDB collects all versions of a key from a MemDB
func (db *DB) collectVersionsFromMemDB(mdb *memdb.DB, key []byte, minVersion, maxVersion uint64, seq uint64, versionMap map[uint64][]byte) {
	// Create an iterator over the MemDB
	iter := mdb.NewIterator(nil)
	defer iter.Release()

	// Seek to the first possible version of this key
	seekKey := dbkey.MakeInternalKeyWithVersion(nil, key, dbkey.LastestVersion, seq, dbkey.KeyTypeSeek)
	if !iter.Seek(seekKey) {
		return
	}

	// Iterate and collect all matching versions
	for ; iter.Valid(); iter.Next() {
		ikey := iter.Key()

		// Parse as versioned key (all keys must be versioned)
		ukey, version, _, kt, err := dbkey.ParseInternalKeyWithVersion(ikey)
		if err != nil {
			break
		}

		// Check if it's the same user key
		if db.s.icmp.uCompare(ukey, key) != 0 {
			break // Moved past this key
		}

		// Check version range
		if minVersion > 0 && version < minVersion {
			continue
		}
		if maxVersion > 0 && version > maxVersion {
			continue
		}

		// Skip deleted entries
		if kt == dbkey.KeyTypeDel {
			continue
		}

		// Add to version map (MemDB has priority, so only add if not exists)
		if _, exists := versionMap[version]; !exists {
			versionMap[version] = append([]byte(nil), iter.Value()...)
		}
	}
}

func nilIfNotFound(err error) error {
	if err == ErrNotFound {
		return nil
	}
	return err
}

func (db *DB) has(auxm *memdb.DB, auxt tFiles, key []byte, version, seq uint64, ro *opt.ReadOptions) (ret bool, err error) {
	ikey := dbkey.MakeInternalKeyWithVersion(nil, key, version, seq, dbkey.KeyTypeSeek)

	if auxm != nil {
		if ok, _, me := memGet(auxm, ikey, db.s.icmp); ok {
			return me == nil, nilIfNotFound(me)
		}
	}

	em, fm := db.getMems()
	for _, m := range [...]*memDB{em, fm} {
		if m == nil {
			continue
		}
		defer m.decref()

		if ok, _, me := memGet(m.DB, ikey, db.s.icmp); ok {
			return me == nil, nilIfNotFound(me)
		}
	}

	v := db.s.version()
	_, cSched, err := v.get(auxt, ikey, ro, true)
	v.release()
	if cSched {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	if err == nil {
		ret = true
	} else if err == ErrNotFound {
		err = nil
	}
	return
}

// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after Get returns.
func (db *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	// Use KeyMaxSeq as version to indicate "latest version"
	return db.get(nil, nil, key, dbkey.LastestVersion, se.seq, ro)
}

// GetWithVersion gets the value for the given key at the specified version.
// If version is 0, it returns the latest version.
// It returns ErrNotFound if the DB does not contain the key at the specified version.
//
// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after GetWithVersion returns.
func (db *DB) GetWithVersion(key []byte, version uint64, ro *opt.ReadOptions) (value []byte, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	return db.get(nil, nil, key, version, se.seq, ro)
}

// GetWithProof gets the value and Merkle proof for the given key at the specified version.
// If version is 0, it returns the latest version.
// It returns ErrNotFound if the DB does not contain the key at the specified version.
//
// The proof can be used to verify that the value is authentic without
// trusting the database. The proof contains the Merkle path from the
// leaf node (containing the key-value pair) to the root hash.
//
// The returned slices are their own copies, it is safe to modify them.
// It is safe to modify the contents of the argument after GetWithProof returns.
//
// Returns:
//   - value: the value of the key
//   - actualVersion: the actual version of the returned value (may differ from requested version if querying latest)
//   - proof: the Merkle proof
//   - err: error if any
func (db *DB) GetWithProof(key []byte, version uint64, ro *opt.ReadOptions) (value []byte, actualVersion uint64, proof *DBProof, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)

	return db.getWithProof(nil, nil, key, version, se.seq, ro)
}

// VersionEntry represents a single version entry for a key.
// It contains the version number, the corresponding value, and the Merkle proof.
type VersionEntry struct {
	Version uint64   // Version number (block number)
	Value   []byte   // Value at this version
	Proof   *DBProof // Merkle proof for this version (nil if not requested)
}

// GetVersionHistory queries all versions of a key within a version range.
// This is used for provenance queries.
func (db *DB) GetVersionHistory(key []byte, minVersion, maxVersion uint64, ro *opt.ReadOptions) (entries []VersionEntry, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	return db.getVersionHistory(nil, nil, key, minVersion, maxVersion, se.seq, ro, false)
}

// GetVersionHistoryWithProof queries all versions of a key within a version range with Merkle proofs.
// This is used for provenance queries with cryptographic verification
func (db *DB) GetVersionHistoryWithProof(key []byte, minVersion, maxVersion uint64, ro *opt.ReadOptions) (entries []VersionEntry, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	return db.getVersionHistory(nil, nil, key, minVersion, maxVersion, se.seq, ro, true)
}

// Has returns true if the DB does contains the given key.
//
// It is safe to modify the contents of the argument after Has returns.
func (db *DB) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	return db.has(nil, nil, key, dbkey.LastestVersion, se.seq, ro)
}

// NewIterator returns an iterator for the latest snapshot of the
// underlying DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (db *DB) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	if err := db.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}

	se := db.acquireSnapshot()
	defer db.releaseSnapshot(se)
	// Iterator holds 'version' lock, 'version' is immutable so snapshot
	// can be released after iterator created.
	return db.newIterator(nil, nil, se.seq, slice, ro)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (db *DB) GetSnapshot() (*Snapshot, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	return db.newSnapshot(), nil
}

// GetProperty returns value of the given property name.
//
// Property names:
//
//	leveldb.num-files-at-level{n}
//		Returns the number of files at level 'n'.
//	leveldb.stats
//		Returns statistics of the underlying DB.
//	leveldb.iostats
//		Returns statistics of effective disk read and write.
//	leveldb.writedelay
//		Returns cumulative write delay caused by compaction.
//	leveldb.sstables
//		Returns sstables list for each level.
//	leveldb.blockpool
//		Returns block pool stats.
//	leveldb.cachedblock
//		Returns size of cached block.
//	leveldb.openedtables
//		Returns number of opened tables.
//	leveldb.alivesnaps
//		Returns number of alive snapshots.
//	leveldb.aliveiters
//		Returns number of alive iterators.
func (db *DB) GetProperty(name string) (value string, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	const prefix = "leveldb."
	if !strings.HasPrefix(name, prefix) {
		return "", ErrNotFound
	}
	p := name[len(prefix):]

	v := db.s.version()
	defer v.release()

	numFilesPrefix := "num-files-at-level"
	switch {
	case strings.HasPrefix(p, numFilesPrefix):
		var level uint
		var rest string
		n, _ := fmt.Sscanf(p[len(numFilesPrefix):], "%d%s", &level, &rest)
		if n != 1 {
			err = ErrNotFound
		} else {
			value = fmt.Sprint(v.tLen(int(level)))
		}
	case p == "stats":
		value = "Compactions\n" +
			" Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)\n" +
			"-------+------------+---------------+---------------+---------------+---------------\n"
		var totalTables int
		var totalSize, totalRead, totalWrite int64
		var totalDuration time.Duration
		for level, tables := range v.levels {
			duration, read, write := db.compStats.getStat(level)
			if len(tables) == 0 && duration == 0 {
				continue
			}
			totalTables += len(tables)
			totalSize += tables.size()
			totalRead += read
			totalWrite += write
			totalDuration += duration
			value += fmt.Sprintf(" %3d   | %10d | %13.5f | %13.5f | %13.5f | %13.5f\n",
				level, len(tables), float64(tables.size())/1048576.0, duration.Seconds(),
				float64(read)/1048576.0, float64(write)/1048576.0)
		}
		value += "-------+------------+---------------+---------------+---------------+---------------\n"
		value += fmt.Sprintf(" Total | %10d | %13.5f | %13.5f | %13.5f | %13.5f\n",
			totalTables, float64(totalSize)/1048576.0, totalDuration.Seconds(),
			float64(totalRead)/1048576.0, float64(totalWrite)/1048576.0)
	case p == "compcount":
		value = fmt.Sprintf("MemComp:%d Level0Comp:%d NonLevel0Comp:%d SeekComp:%d", atomic.LoadUint32(&db.memComp), atomic.LoadUint32(&db.level0Comp), atomic.LoadUint32(&db.nonLevel0Comp), atomic.LoadUint32(&db.seekComp))
	case p == "iostats":
		value = fmt.Sprintf("Read(MB):%.5f Write(MB):%.5f",
			float64(db.s.stor.reads())/1048576.0,
			float64(db.s.stor.writes())/1048576.0)
	case p == "writedelay":
		writeDelayN, writeDelay := atomic.LoadInt32(&db.cWriteDelayN), time.Duration(atomic.LoadInt64(&db.cWriteDelay))
		paused := atomic.LoadInt32(&db.inWritePaused) == 1
		value = fmt.Sprintf("DelayN:%d Delay:%s Paused:%t", writeDelayN, writeDelay, paused)
	case p == "sstables":
		for level, tables := range v.levels {
			value += fmt.Sprintf("--- level %d ---\n", level)
			for _, t := range tables {
				value += fmt.Sprintf("%d:%d[%q .. %q]\n", t.fd.Num, t.size, t.imin, t.imax)
			}
		}
	case p == "blockpool":
		value = fmt.Sprintf("%v", db.s.tops.blockBuffer)
	case p == "cachedblock":
		if db.s.tops.blockCache != nil {
			value = fmt.Sprintf("%d", db.s.tops.blockCache.Size())
		} else {
			value = "<nil>"
		}
	case p == "openedtables":
		value = fmt.Sprintf("%d", db.s.tops.fileCache.Size())
	case p == "alivesnaps":
		value = fmt.Sprintf("%d", atomic.LoadInt32(&db.aliveSnaps))
	case p == "aliveiters":
		value = fmt.Sprintf("%d", atomic.LoadInt32(&db.aliveIters))
	default:
		err = ErrNotFound
	}

	return
}

// DBStats is database statistics.
type DBStats struct {
	WriteDelayCount    int32
	WriteDelayDuration time.Duration
	WritePaused        bool

	AliveSnapshots int32
	AliveIterators int32

	IOWrite uint64
	IORead  uint64

	BlockCacheSize    int
	OpenedTablesCount int

	FileCache  cache.Stats
	BlockCache cache.Stats

	LevelSizes        Sizes
	LevelTablesCounts []int
	LevelRead         Sizes
	LevelWrite        Sizes
	LevelDurations    []time.Duration

	MemComp       uint32
	Level0Comp    uint32
	NonLevel0Comp uint32
	SeekComp      uint32
}

// Stats populates s with database statistics.
func (db *DB) Stats(s *DBStats) error {
	err := db.ok()
	if err != nil {
		return err
	}

	s.IORead = db.s.stor.reads()
	s.IOWrite = db.s.stor.writes()
	s.WriteDelayCount = atomic.LoadInt32(&db.cWriteDelayN)
	s.WriteDelayDuration = time.Duration(atomic.LoadInt64(&db.cWriteDelay))
	s.WritePaused = atomic.LoadInt32(&db.inWritePaused) == 1

	s.OpenedTablesCount = db.s.tops.fileCache.Size()
	if db.s.tops.blockCache != nil {
		s.BlockCacheSize = db.s.tops.blockCache.Size()
	} else {
		s.BlockCacheSize = 0
	}

	s.FileCache = db.s.tops.fileCache.GetStats()
	if db.s.tops.blockCache != nil {
		s.BlockCache = db.s.tops.blockCache.GetStats()
	} else {
		s.BlockCache = cache.Stats{}
	}

	s.AliveIterators = atomic.LoadInt32(&db.aliveIters)
	s.AliveSnapshots = atomic.LoadInt32(&db.aliveSnaps)

	s.LevelDurations = s.LevelDurations[:0]
	s.LevelRead = s.LevelRead[:0]
	s.LevelWrite = s.LevelWrite[:0]
	s.LevelSizes = s.LevelSizes[:0]
	s.LevelTablesCounts = s.LevelTablesCounts[:0]

	v := db.s.version()
	defer v.release()

	for level, tables := range v.levels {
		duration, read, write := db.compStats.getStat(level)

		s.LevelDurations = append(s.LevelDurations, duration)
		s.LevelRead = append(s.LevelRead, read)
		s.LevelWrite = append(s.LevelWrite, write)
		s.LevelSizes = append(s.LevelSizes, tables.size())
		s.LevelTablesCounts = append(s.LevelTablesCounts, len(tables))
	}
	s.MemComp = atomic.LoadUint32(&db.memComp)
	s.Level0Comp = atomic.LoadUint32(&db.level0Comp)
	s.NonLevel0Comp = atomic.LoadUint32(&db.nonLevel0Comp)
	s.SeekComp = atomic.LoadUint32(&db.seekComp)
	return nil
}

// SizeOf calculates approximate sizes of the given key ranges.
// The length of the returned sizes are equal with the length of the given
// ranges. The returned sizes measure storage space usage, so if the user
// data compresses by a factor of ten, the returned sizes will be one-tenth
// the size of the corresponding user data size.
// The results may not include the sizes of recently written data.
func (db *DB) SizeOf(ranges []util.Range) (Sizes, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	v := db.s.version()
	defer v.release()

	sizes := make(Sizes, 0, len(ranges))
	for _, r := range ranges {
		imin := dbkey.MakeInternalKey(nil, r.Start, dbkey.KeyMaxSeq, dbkey.KeyTypeSeek)
		imax := dbkey.MakeInternalKey(nil, r.Limit, dbkey.KeyMaxSeq, dbkey.KeyTypeSeek)
		start, err := v.offsetOf(imin)
		if err != nil {
			return nil, err
		}
		limit, err := v.offsetOf(imax)
		if err != nil {
			return nil, err
		}
		var size int64
		if limit >= start {
			size = limit - start
		}
		sizes = append(sizes, size)
	}

	return sizes, nil
}

func (db *DB) ComputeMasterRoot(v *version) merkle.Hash {
	defer v.release()

	// Collect layer roots from all layers
	// Order: [MemDB root, Level0 root, Level1 root, ...]
	// Same order as buildMemDBProof and generateMasterProof
	var layerRoots []merkle.Hash

	// Add MemDB root if available (same as buildMemDBProof and generateMasterProof)
	em, fm := db.getMems()
	for _, m := range [...]*memDB{em, fm} {
		if m == nil {
			continue
		}
		defer m.decref()

		if m.Len() > 0 {
			// Build MemDB snapshot to get root
			memRoot := m.DB.GetMerkleRoot()
			layerRoots = append(layerRoots, memRoot)
			db.logf("master@root memdb layer_root=%x num_entries=%d", memRoot[:8], m.Len())
			break // Only use the first non-empty memdb
		}
	}

	// Process each level to collect layer roots
	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		// Collect all SST roots in this level
		var sstRoots []merkle.Hash
		for _, t := range tables {
			// Try to get Merkle root from table
			if root, err := db.s.tops.getMerkleRoot(t); err == nil {
				sstRoots = append(sstRoots, root)
				// db.logf("master@root level=%d file=%d root=%x", level, t.fd.Num, root[:8])
			}
		}

		if len(sstRoots) > 0 {
			// Build Merkle tree from SST roots to create this layer's root
			// This is the KEY change: use BuildTreeFromHashes instead of AggregateRoots
			layerRoot := merkle.BuildTreeFromHashes(sstRoots)
			layerRoots = append(layerRoots, layerRoot)
			db.logf("master@root level=%d layer_root=%x num_ssts=%d", level, layerRoot[:8], len(sstRoots))
		}
	}
	if len(layerRoots) == 0 {
		return merkle.Hash{} // Empty hash
	}
	// Build final Merkle tree from all layer roots to create MasterRoot
	// This is the top-level aggregation
	masterRoot := merkle.BuildTreeFromHashes(layerRoots)
	db.logf("master@root final master_root=%x num_layers=%d", masterRoot[:8], len(layerRoots))
	return masterRoot
}

// generateMasterProof generates a Merkle proof for a layer within the master tree.
// The proof shows that a specific layer's root is part of the master tree.
// layerProof.Root is used to identify which layer we're proving.
func (db *DB) generateMasterProof(v *version, currentLayerRoot merkle.Hash) (*merkle.MerkleProof, error) {
	// Collect all layer roots in the same order as buildMemDBProof
	// Order: [MemDB root, Level0 root, Level1 root, ...]
	var layerRoots []merkle.Hash
	targetIndex := -1

	// Add MemDB root if available (same as buildMemDBProof)
	em, fm := db.getMems()
	for _, m := range [...]*memDB{em, fm} {
		if m == nil {
			continue
		}
		defer m.decref()

		if m.Len() > 0 {
			// Build MemDB snapshot to get root
			memRoot := m.DB.GetMerkleRoot()

			// Check if memdb is the target layer
			if memRoot.Equal(currentLayerRoot) {
				targetIndex = len(layerRoots)
			}

			layerRoots = append(layerRoots, memRoot)
			db.logf("master@proof memdb layer_root=%x num_entries=%d", memRoot[:8], m.Len())
			break // Only use the first non-empty memdb
		}
	}

	// Process each level to collect layer roots (same order as buildMemDBProof)
	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		// Collect all SST roots in this level
		var sstRoots []merkle.Hash
		for _, t := range tables {
			if root, err := db.s.tops.getMerkleRoot(t); err == nil {
				sstRoots = append(sstRoots, root)
			}
		}

		if len(sstRoots) > 0 {
			// Build Merkle tree from SST roots to create this layer's root
			layerRoot := merkle.BuildTreeFromHashes(sstRoots)

			// Check if this is the target layer
			if layerRoot.Equal(currentLayerRoot) {
				targetIndex = len(layerRoots)
			}

			layerRoots = append(layerRoots, layerRoot)
			db.logf("master@proof level=%d layer_root=%x num_ssts=%d", level, layerRoot[:8], len(sstRoots))
		}
	}

	if len(layerRoots) == 0 {
		return nil, nil
	}

	if targetIndex < 0 {
		// Layer root not found in current version
		// This can happen if the version changed between queries
		db.logf("master@proof layer root not found: %x", currentLayerRoot)
		return nil, nil
	}

	// Build master tree from layer roots
	masterTree := merkle.NewMerkleTree(layerRoots)

	// Generate proof for the target layer
	masterProof, err := masterTree.GenerateProof(targetIndex)
	if err != nil {
		return nil, err
	}

	db.logf("master@proof generated for layer_index=%d total_layers=%d", targetIndex, len(layerRoots))
	return masterProof, nil
}

// Close closes the DB. This will also releases any outstanding snapshot,
// abort any in-flight compaction and discard open transaction.
//
// It is not safe to close a DB until all outstanding iterators are released.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (db *DB) Close() error {
	if !db.setClosed() {
		return ErrClosed
	}

	start := time.Now()
	db.log("db@close closing")

	// Clear the finalizer.
	runtime.SetFinalizer(db, nil)

	// Get compaction error.
	var err error
	select {
	case err = <-db.compErrC:
		if err == ErrReadOnly {
			err = nil
		}
	default:
	}

	// Signal all goroutines.
	close(db.closeC)

	// Discard open transaction.
	if db.tr != nil {
		db.tr.Discard()
	}

	// Acquire writer lock.
	db.writeLockC <- struct{}{}

	// Wait for all gorotines to exit.
	db.closeW.Wait()

	// Closes journal.
	if db.journal != nil {
		db.journal.Close()
		db.journalWriter.Close()
		db.journal = nil
		db.journalWriter = nil
	}

	if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
	}

	// Close session.
	db.s.close()
	db.logf("db@close done T·%v", time.Since(start))
	db.s.release()

	if db.closer != nil {
		if err1 := db.closer.Close(); err == nil {
			err = err1
		}
		db.closer = nil
	}

	// Clear memdbs.
	db.clearMems()

	return err
}
