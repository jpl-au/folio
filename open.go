// Database creation and opening.
package folio

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jpl-au/folio/internal/bloom"
	"github.com/jpl-au/folio/internal/flock"
)

// Open opens or creates a database at the given path. If a previous
// session crashed (dirty flag set, or .tmp file left behind), an automatic
// Repair is attempted under an exclusive lock to restore consistency
// before returning.
func Open(path string, config Config) (*DB, error) {
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	if config.HashAlgorithm == 0 {
		config.HashAlgorithm = AlgXXHash3
	}
	switch config.HashAlgorithm {
	case AlgXXHash3, AlgFNV1a, AlgBlake2b:
		// valid
	default:
		return nil, fmt.Errorf("open: unknown hash algorithm: %d", config.HashAlgorithm)
	}
	if config.ReadBuffer == 0 {
		config.ReadBuffer = 64 * 1024
	}
	if config.MaxRecordSize == 0 {
		config.MaxRecordSize = 16 * 1024 * 1024
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}

	_, err = root.Stat(name)
	if os.IsNotExist(err) {
		file, err := root.Create(name)
		if err != nil {
			root.Close()
			return nil, err
		}
		hdr := Header{
			Version:   1,
			Timestamp: now(),
			Algorithm: config.HashAlgorithm,
		}
		hdr.State[stThreshold] = uint64(config.AutoCompact)
		buf, err := hdr.encode()
		if err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("encode header: %w", err)
		}
		if _, err := file.Write(buf); err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("sync header: %w", err)
		}
		file.Close()
	}

	reader, err := root.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		root.Close()
		return nil, err
	}

	writer, err := root.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		reader.Close()
		root.Close()
		return nil, err
	}

	fl := flock.New(writer)

	info, err := writer.Stat()
	if err != nil {
		reader.Close()
		writer.Close()
		root.Close()
		return nil, fmt.Errorf("stat: %w", err)
	}
	hdr, err := header(reader)
	if err != nil {
		reader.Close()
		writer.Close()
		root.Close()
		return nil, err
	}

	db := &DB{
		root:   root,
		name:   name,
		reader: reader,
		writer: writer,
		lock:   fl,
		header: hdr,
		config: config,
		tail:   info.Size(),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
	db.count.Store(hdr.State[stCount])

	// A non-zero AutoCompact is a deliberate change — persist it to the
	// header so it survives future opens without needing to be repeated.
	if config.AutoCompact > 0 && uint64(config.AutoCompact) != hdr.State[stThreshold] {
		db.header.State[stThreshold] = uint64(config.AutoCompact)
		hdrBytes, err := db.header.encode()
		if err != nil {
			reader.Close()
			writer.Close()
			root.Close()
			return nil, fmt.Errorf("encode header: %w", err)
		}
		if _, err := writer.WriteAt(hdrBytes, 0); err != nil {
			reader.Close()
			writer.Close()
			root.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
	}

	if config.BloomFilter {
		db.bloom = bloom.New()
		s := source{reader, info.Size()}
		entries := scanm(s, db.sparseStart(), info.Size(), TypeIndex)
		for _, e := range entries {
			db.bloom.Add(e.ID)
		}
	}

	if config.Index {
		s := source{reader, info.Size()}
		entries := scanm(s, HeaderSize, info.Size(), TypeIndex)
		db.index = make(map[string]int64, len(entries))
		for _, e := range entries {
			db.index[e.ID] = e.SrcOff
		}
	}

	if config.MMap {
		data, err := mmapFile(reader, info.Size())
		if err != nil {
			// Unsupported platform or empty file — fall back to file I/O.
			db.config.MMap = false
		} else {
			db.mapped = data
		}
	}

	// A leftover .tmp file or a dirty header means the previous session
	// crashed mid-write. Repair rebuilds the file from its surviving records.
	_, tmpErr := root.Stat(name + ".tmp")
	tmpExists := tmpErr == nil
	needsRepair := tmpExists || db.header.Error == 1

	if needsRepair {
		if tmpExists {
			root.Remove(name + ".tmp")
		}
		// Attempt to acquire exclusive lock for repair
		if err := db.lock.Acquire(flock.Exclusive); err == nil {
			defer db.lock.Release()
			db.Repair(&CompactOptions{BlockReaders: true})
		}
	}

	return db, nil
}
