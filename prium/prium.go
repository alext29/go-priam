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

// Init initializes individual components of Prium object.
func (p *Prium) Init() error {

	if err := p.cassandra.Init(); err != nil {
		return errors.Wrap(err, "failed cassandra init")
	}

	if err := p.s3.Init(); err != nil {
		return errors.Wrap(err, "failed s3 init")
	}
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

	env := p.config.Env
	keyspace := p.config.Keyspace

	// get snapshot history from S3
	h, err := p.s3.GetSnapshotHistory(env, keyspace)
	if err != nil {
		return errors.Wrap(err, "error getting snapshot history")
	}

	// get timestamp
	timestamp := NewTimestamp()
	glog.Infof("generating snapshot with timestamp: %s", timestamp)

	// get parent timestamp
	parent := timestamp
	snapshots := h.List()
	if len(snapshots) > 0 && p.config.Incremental {
		parent = snapshots[len(snapshots)-1]
	} else {
		p.config.Incremental = false
	}
	glog.Infof("timestamp of parent snapshot: %s", parent)

	// take snapshot on each host
	// TODO: do in parallel
	for _, host := range hosts {
		glog.Infof("take snapshot on cassandra host: %s", host)

		// create snapshot
		files, dirs, err := p.cassandra.Snapshot(host, timestamp, keyspace)
		if err != nil {
			glog.Errorf("error taking snapshot on host %s :: %v", host, err)
			continue
		}

		// upload files to s3
		if err = p.s3.UploadFiles(env, keyspace, parent, timestamp, host, files); err != nil {
			glog.Errorf("error uploading files from host %s :: %v", host, err)
		}

		// delete local files
		if err = p.cassandra.deleteSnapshot(host, dirs); err != nil {
			glog.Errorf("error deleting snapshot on host %s :: %v", host, err)
			continue
		}
	}
	return nil
}

// NewTimestamp generates a new timestamp which is current Unix time, i.e
// seconds elapsed since 1st January 1970. The code assumes timestamps
// are monotonically increasing and is used by restore function to determine
// which backup is the latest as well as the order of incremental backups.
// TODO: barf if timestamp is older than the last backup.
func NewTimestamp() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

// Restore cassandra from a given snapshot.
func (p *Prium) Restore() error {

	// get all cassandra hosts
	hosts := p.cassandra.Hosts()
	if len(hosts) == 0 {
		return fmt.Errorf("did not find valid cassandra hosts")
	}

	env := p.config.Env
	keyspace := p.config.Keyspace
	snapshot := p.config.Snapshot
	localTmpDir := fmt.Sprintf("%s/local", p.config.Prefix)
	remoteTmpDir := fmt.Sprintf("%s/remote", p.config.Prefix)

	// get snapshot history from S3
	h, err := p.s3.GetSnapshotHistory(env, keyspace)
	if err != nil {
		return errors.Wrap(err, "failed to get backup history")
	}

	if snapshot == "" {
		snapshots := h.List()
		if len(snapshots) > 0 {
			snapshot = snapshots[len(snapshots)-1]
		}
	}
	if snapshot == "" {
		return fmt.Errorf("no existing backup to restore from")
	}

	if !h.Valid(snapshot) {
		return fmt.Errorf("%s is not a valid snapshot", snapshot)
	}
	glog.Infof("restoring to snapshot: %s", snapshot)

	keys, err := h.Keys(snapshot)
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
