package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"path"
	"time"
)

// Prium object provides backup and restore of cassandra DB to AWS S3.
type Prium struct {
	agent     *Agent
	cassandra *Cassandra
	config    *Config
	s3        *S3
	hist      *SnapshotHistory
}

// New returns a new Prium object.
func New(config *Config) *Prium {
	agent := NewAgent(config)
	return &Prium{
		agent:     agent,
		config:    config,
		cassandra: NewCassandra(config, agent),
		s3:        NewS3(config, agent),
	}
}

// History prints the current list of backups in S3.
func (p *Prium) History() error {

	// get snapshot history
	if err := p.SnapshotHistory(); err != nil {
		return errors.Wrap(err, "error getting snapshot history")
	}
	glog.Infof("current backups:\n%s", p.hist)
	return nil
}

// Backup flushes all cassandra tables to disk identifies the appropriate
// files and copies them to the specified AWS S3 bucket.
func (p *Prium) Backup() error {

	glog.Infof("start taking backup...")

	// get all cassandra hosts
	hosts := p.cassandra.Hosts()
	if len(hosts) == 0 {
		return fmt.Errorf("unable to get any cassandra hosts")
	}

	// get snapshot history
	if err := p.SnapshotHistory(); err != nil {
		return errors.Wrap(err, "error getting snapshot history")
	}

	// generate new timestamp
	timestamp := p.NewTimestamp()
	glog.Infof("generating snapshot with timestamp: %s", timestamp)

	// get parent timestamp
	parent := timestamp
	snapshots := p.hist.List()

	// check timestamps are monotonically increasing
	if len(snapshots) > 0 && snapshots[len(snapshots)-1] > timestamp {
		return fmt.Errorf("new timestamp %s less than last", timestamp)
	}

	// assign parent timestamp if incremental
	if len(snapshots) > 0 && p.config.Incremental {
		parent = snapshots[len(snapshots)-1]
	} else {
		p.config.Incremental = false
	}
	glog.Infof("timestamp of parent snapshot: %s", parent)

	// take snapshot on each host
	// TODO: do in parallel
	for _, host := range hosts {
		glog.Infof("snapshot @ %s", host)

		// create snapshot
		files, dirs, err := p.cassandra.Snapshot(host, timestamp)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("snapshot @ %s", host))
		}

		// upload files to s3
		if err = p.s3.UploadFiles(parent, timestamp, host, files); err != nil {
			return errors.Wrap(err, fmt.Sprintf("upload @ %s", host))
		}

		// delete local files
		if err = p.cassandra.deleteSnapshot(host, dirs); err != nil {
			return errors.Wrap(err, fmt.Sprintf("delete @ %s", host))
		}
	}
	return nil
}

// SnapshotHistory returns snapshot history
func (p *Prium) SnapshotHistory() error {
	if p.hist != nil {
		return nil
	}
	// get snapshot history from S3 if not already present
	h, err := p.s3.SnapshotHistory()
	if err != nil {
		return errors.Wrap(err, "error getting snapshot history")
	}
	p.hist = h
	return nil
}

// NewTimestamp generates a new timestamp which is based on current time.
// The code assumes timestamps are monotonically increasing and is used by
// restore function to determine which backup is the latest as well as the
// order of incremental backups.
func (p *Prium) NewTimestamp() string {
	return time.Now().Format("2006-01-02_15:04:05")
}

// Restore cassandra from a given snapshot.
// TODO: if restoring from a cassandra node then skip copying file to
// cassandra host.
func (p *Prium) Restore() error {

	// get all cassandra hosts
	hosts := p.cassandra.Hosts()
	if len(hosts) == 0 {
		return fmt.Errorf("did not find valid cassandra hosts")
	}

	snapshot := p.config.Snapshot
	localTmpDir := fmt.Sprintf("%s/local", p.config.TempDir)
	remoteTmpDir := fmt.Sprintf("%s/remote", p.config.TempDir)

	// get snapshot history
	if err := p.SnapshotHistory(); err != nil {
		return err
	}

	if snapshot == "" {
		snapshots := p.hist.List()
		if len(snapshots) > 0 {
			snapshot = snapshots[len(snapshots)-1]
		}
	}
	if snapshot == "" {
		return fmt.Errorf("no existing backup to restore from")
	}

	if !p.hist.Valid(snapshot) {
		return fmt.Errorf("%s is not a valid snapshot", snapshot)
	}
	glog.Infof("restoring to snapshot: %s", snapshot)

	keys, err := p.hist.Keys(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to get all keys")
	}

	files, err := p.s3.downloadKeys(keys, localTmpDir)
	if err != nil {
		return errors.Wrap(err, "error downloading keys")
	}

	// upload files to first availanle host
	dirs, err := p.uploadFilesToHost(hosts[0], remoteTmpDir, files)
	if err != nil {
		return errors.Wrap(err, "could not upload files to host")
	}

	// take snapshot on each host
	err = p.cassandra.sstableload(hosts[0], dirs)
	if err != nil {
		return errors.Wrap(err, "failed to run sstableloader")
	}

	return nil
}

// uploadFilesToHost copies cassandra failes to a local directory on
// one of the cassandra hosts.
func (p *Prium) uploadFilesToHost(host, remoteTmpDir string, files map[string]string) (map[string]bool, error) {
	dirs := make(map[string]bool)
	for key, localFile := range files {
		glog.V(2).Infof("copy to %s: %s", host, key)
		remoteDir := path.Dir(fmt.Sprintf("%s/%s", remoteTmpDir, key))
		err := p.agent.UploadFile(host, localFile, remoteDir)
		if err != nil {
			return nil, errors.Wrap(err, "error uploading backup files to host")
		}
		dirs[remoteDir] = true
	}
	return dirs, nil
}
