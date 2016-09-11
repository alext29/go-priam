package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"path"
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
	localTmpDir := fmt.Sprintf("%s/local", *conf.prefix)
	remoteTmpDir := fmt.Sprintf("%s/remote", *conf.prefix)

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

	files, err := s3.downloadKeys(keys, localTmpDir)
	if err != nil {
		return errors.Wrap(err, "error downloading keys")
	}

	// upload files to first availanle host
	dirs, err := uploadFilesToHost(conf, hosts[0], remoteTmpDir, files)
	if err != nil {
		return errors.Wrap(err, "could not upload files to host")
	}

	glog.Infof("directories: %s", dirs)

	// take snapshot on each host
	err = db.sstableload(hosts[0], dirs)
	if err != nil {
		return errors.Wrap(err, "failed to run sstableloader")
	}

	return nil
}

func uploadFilesToHost(conf *Config, host, remoteDir string, files map[string]string) (map[string]bool, error) {

	a := newAgent(conf)
	dirs := make(map[string]bool)

	for key, localFile := range files {
		glog.Infof("key: %s", key)
		glog.Infof("file: %s", localFile)
		remoteDir := path.Dir(fmt.Sprintf("%s/%s", remoteDir, key))
		glog.Infof("remote file: %s", remoteDir)
		err := a.uploadFile(host, localFile, remoteDir)
		if err != nil {
			return nil, errors.Wrap(err, "error uploading backup files to host")
		}
		dirs[remoteDir] = true
	}

	return dirs, nil
}
