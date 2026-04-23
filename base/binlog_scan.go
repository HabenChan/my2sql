package base

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

// GetBinlogFirstTimestamp reads only event headers from a local binlog file to
// find the first event with a non-zero timestamp. Scans at most 30 event headers
// (19 bytes each), so total I/O is under 1 KB per file in the common case.
func GetBinlogFirstTimestamp(filePath string) (uint32, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	magic := make([]byte, 4)
	if _, err = io.ReadFull(f, magic); err != nil {
		return 0, fmt.Errorf("read magic bytes: %v", err)
	}
	if !bytes.Equal(magic, replication.BinLogFileHeader) {
		return 0, fmt.Errorf("not a valid binlog file")
	}

	for i := 0; i < 30; i++ {
		hdr := make([]byte, replication.EventHeaderSize)
		if _, err = io.ReadFull(f, hdr); err != nil {
			break
		}
		ts := binary.LittleEndian.Uint32(hdr[0:4])
		eventSize := binary.LittleEndian.Uint32(hdr[9:13])
		if eventSize < uint32(replication.EventHeaderSize) {
			break
		}
		if ts > 0 {
			return ts, nil
		}
		bodySize := int64(eventSize) - int64(replication.EventHeaderSize)
		if bodySize > 0 {
			if _, err = io.CopyN(io.Discard, f, bodySize); err != nil {
				break
			}
		}
	}
	return 0, nil
}

// GetBinlogFirstTimestampFromRepl connects to MySQL and peeks at the first
// non-zero event timestamp in a remote binlog file using a short-lived
// BinlogSyncer. Uses server ID 4294967000 (near uint32 max) to avoid
// conflicting with the primary replication server ID.
func GetBinlogFirstTimestampFromRepl(cfg *ConfCmd, binlogFile string) (uint32, error) {
	probeCfg := replication.BinlogSyncerConfig{
		ServerID:                4294967000,
		Flavor:                  cfg.MysqlType,
		Host:                    cfg.Host,
		Port:                    uint16(cfg.Port),
		User:                    cfg.User,
		Password:                cfg.Passwd,
		Charset:                 "utf8",
		ParseTime:               false,
		UseDecimal:              false,
		TimestampStringLocation: GBinlogTimeLocation,
	}
	syncer := replication.NewBinlogSyncer(probeCfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: binlogFile, Pos: 4})
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			break
		}
		if ev.Header.Timestamp > 0 {
			return ev.Header.Timestamp, nil
		}
	}
	return 0, nil
}

// GetBinlogFileListFromDB queries SHOW BINARY LOGS and returns filenames in server order.
func GetBinlogFileListFromDB(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var files []string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		switch v := vals[0].(type) {
		case []byte:
			files = append(files, string(v))
		case string:
			files = append(files, v)
		}
	}
	return files, nil
}

// GetBinlogFileListFromDir returns sorted binlog base filenames in dir whose
// names match "<baseName>.<numeric-index>" (e.g. "mysql-bin.000001").
func GetBinlogFileListFromDir(dir, baseName string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	prefix := baseName + "."
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if _, err2 := strconv.ParseUint(suffix, 10, 64); err2 == nil {
			files = append(files, name)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		_, idxI := GetBinlogBasenameAndIndex(files[i])
		_, idxJ := GetBinlogBasenameAndIndex(files[j])
		return idxI < idxJ
	})
	return files, nil
}

// BinarySearchBinlogFileMode returns the index into files[] of the last file
// whose first-event timestamp is <= startTime, reading local file headers.
// Returns 0 when all files have timestamps after startTime (safe fallback).
func BinarySearchBinlogFileMode(files []string, dir string, startTime uint32) int {
	result := 0
	lo, hi := 0, len(files)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		fullPath := filepath.Join(dir, files[mid])
		ts, err := GetBinlogFirstTimestamp(fullPath)
		if err != nil {
			log.Warnf("binary search: cannot read %s: %v", fullPath, err)
			hi = mid - 1
			continue
		}
		log.Infof("binary search probe: %s first_ts=%d target=%d", files[mid], ts, startTime)
		if ts == 0 || ts <= startTime {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// BinarySearchBinlogReplMode returns the index into files[] of the last file
// whose first-event timestamp is <= startTime, probing via short-lived BinlogSyncer.
// Returns 0 when all files have timestamps after startTime (safe fallback).
func BinarySearchBinlogReplMode(cfg *ConfCmd, files []string, startTime uint32) int {
	result := 0
	lo, hi := 0, len(files)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		ts, err := GetBinlogFirstTimestampFromRepl(cfg, files[mid])
		if err != nil {
			log.Warnf("binary search repl: cannot probe %s: %v", files[mid], err)
			hi = mid - 1
			continue
		}
		log.Infof("binary search probe: %s first_ts=%d target=%d", files[mid], ts, startTime)
		if ts == 0 || ts <= startTime {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// AutoDetectReplStartFile queries SHOW BINARY LOGS, binary-searches for the
// binlog file covering cfg.StartDatetime, and updates cfg accordingly.
func AutoDetectReplStartFile(cfg *ConfCmd) error {
	files, err := GetBinlogFileListFromDB(cfg.FromDB)
	if err != nil {
		return fmt.Errorf("list binary logs: %v", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no binary logs found on server")
	}
	log.Infof("binary search: %d binlog files on server", len(files))
	idx := BinarySearchBinlogReplMode(cfg, files, cfg.StartDatetime)
	cfg.StartFile = files[idx]
	cfg.StartPos = 4
	cfg.StartFilePos = mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	cfg.IfSetStartFilePos = true
	log.Infof("binary search result: start from %s (file %d/%d, skipped %d files)",
		cfg.StartFile, idx+1, len(files), idx)
	return nil
}
