package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"time"
)

// Backup takes a backup of cassandra DB
func Backup(conf *Config, db *Cassandra, s3 *S3) error {
	glog.Infof("taking backup..")

	// get all cassandra hosts
	hosts := db.Hosts()
	if len(hosts) == 0 {
		return fmt.Errorf("did not find valid cassandra hosts")
	}

	env := *conf.env
	keyspace := *conf.keyspace

	// get snapshot history from S3
	h, err := s3.GetSnapshotHistory(env, keyspace)
	if err != nil {
		return errors.Wrap(err, "error getting snapshot history")
	}

	// get timestamp
	timestamp := genTimestamp()
	glog.Infof("generating snapshot with timestamp: %s", timestamp)

	// get parent timestamp
	parent := timestamp
	snapshots := h.List()
	if len(snapshots) > 0 && *conf.incremental {
		parent = snapshots[len(snapshots)-1]
	} else {
		*conf.incremental = false
	}
	glog.Infof("timestamp of parent snapshot: %s", parent)

	// take snapshot on each host
	// TODO: do in parallel
	for _, host := range hosts {
		glog.Infof("take snapshot on cassandra host: %s", host)

		// create snapshot
		files, dirs, err := db.Snapshot(host, timestamp, keyspace)
		if err != nil {
			glog.Errorf("error taking snapshot on host %s :: %v", host, err)
			continue
		}

		// upload files to s3
		if err = s3.UploadFiles(env, keyspace, parent, timestamp, host, files); err != nil {
			glog.Errorf("error uploading files from host %s :: %v", host, err)
		}

		// delete local files
		if err = db.deleteSnapshot(host, dirs); err != nil {
			glog.Errorf("error deleting snapshot on host %s :: %v", host, err)
			continue
		}
	}
	return nil
}

// newTimestamp generates a new timestamp.
func genTimestamp() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}
