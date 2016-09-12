package prium

import (
	"fmt"
	"sort"
	"strings"
)

// SnapshotHistory provides the history of all snapshots in S3 for a given environment and keyspace
type SnapshotHistory struct {
	parent map[string]string   // parent of a snapshot if incremental
	keys   map[string][]string // list of keys for given snapshot
}

// NewSnapshotHistory  ..
func NewSnapshotHistory() *SnapshotHistory {
	return &SnapshotHistory{
		parent: make(map[string]string),
		keys:   make(map[string][]string),
	}
}

// Add key to snapshot history
func (h *SnapshotHistory) Add(key string) {
	parts := strings.Split(key, "/")
	parent := parts[2]
	timestamp := parts[3]
	if parent != timestamp {
		h.parent[timestamp] = parent
	}
	h.keys[timestamp] = append(h.keys[timestamp], key)
}

// List returns a ordered list of timestamps.
func (h *SnapshotHistory) List() []string {
	var timestamps []string
	for timestamp := range h.keys {
		timestamps = append(timestamps, timestamp)
	}
	sort.Strings(timestamps)
	return timestamps
}

// Keys returns all keys for a given snapshot including keys for
// parent snapshots if this is an incremental backup
func (h *SnapshotHistory) Keys(snapshot string) ([]string, error) {
	var keys []string
	for {
		k, ok := h.keys[snapshot]
		if !ok {
			return nil, fmt.Errorf("did not find snapshot %s", snapshot)
		}
		keys = append(keys, k...)
		snapshot, ok = h.parent[snapshot]
		if !ok {
			break
		}
	}
	return keys, nil
}

// Valid returns true if this snapshot exists in history
func (h *SnapshotHistory) Valid(snapshot string) bool {
	_, ok := h.keys[snapshot]
	return ok
}

// String representation of snapshot history
func (h *SnapshotHistory) String() string {

	list := h.List()
	if len(list) == 0 {
		return ""
	}
	str := ""
	for _, timestamp := range list {
		if _, ok := h.parent[timestamp]; ok {
			str = fmt.Sprintf("%s     ", str)
		}
		str = fmt.Sprintf("%s+-- %s\n", str, timestamp)
	}
	return str
}
