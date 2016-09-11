package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// Restore cassandra from a given snapshot.
func Restore(conf *Config, db *Cassandra, s3 *S3) error {

	// get all cassandra hosts
	hosts := db.Hosts()
	if len(hosts) == 0 {
		return fmt.Errorf("did not find valid cassandra hosts")
	}

	env := *conf.env
	keyspace := *conf.keyspace
	snapshot := *conf.snapshot
	prefix := *conf.prefix

	// get snapshot history from S3
	h, err := s3.GetSnapshotHistory(env, keyspace)
	if err != nil {
		return errors.Wrap(err, "required command line flags missing")
	}

	if snapshot == "" {
		snapshots := h.List()
		if len(snapshots) > 0 {
			snapshot = snapshots[len(snapshots)-1]
		}
	}
	if snapshot == "" {
		return fmt.Errorf("no valid snapshot to restore to")
	}

	if !h.Valid(snapshot) {
		return fmt.Errorf("%s is not a valid snapshot", snapshot)
	}
	glog.Infof("restoring to snapshot: %s", snapshot)

	keys, err := h.Keys(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to get all keys")
	}

	err = s3.downloadKeys(keys, prefix)
	if err != nil {
		return errors.Wrap(err, "error downloading keys")
	}

	// take snapshot on each host
	return nil
}
