package priam

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"os"
	"regexp"
	"strings"
)

// Cassandra provides methods to interface with a Cassandra cluster.
type Cassandra struct {
	config *Config
	agent  *Agent
}

// NewCassandra returns a new Cassandra object.
func NewCassandra(config *Config, agent *Agent) *Cassandra {
	return &Cassandra{
		config: config,
		agent:  agent,
	}
}

// Hosts returns slice of cassandra hosts
func (c *Cassandra) Hosts() []string {
	cmd := fmt.Sprintf("%s status", c.config.Nodetool)
	bytes, err := c.agent.Run(c.config.Host, cmd)
	if err != nil {
		glog.Errorf("error running cmd '%s' on host '%s' :: %v", cmd, c.config.Host, err)
		return nil
	}
	var hosts []string
	var ws = regexp.MustCompile("( |\t)+")
	for _, line := range strings.Split(string(bytes), "\n") {
		m, err := regexp.MatchString("^(U|N)(N|L|J|M)", line)
		if err != nil {
			glog.Errorf("error matching pattern")
		}
		if m {
			slices := ws.Split(line, 3)
			glog.V(2).Infof("found cassandra node: %s", slices[1])
			hosts = append(hosts, slices[1])
		}
	}
	return hosts
}

// SchemaBackup takes backup of a keyspace and saves it on remote machine
func (c *Cassandra) SchemaBackup(host string) (string, error) {
	file := fmt.Sprintf("/tmp/temp.schema")
	cmd := fmt.Sprintf("echo 'DESCRIBE KEYSPACE %s' | %s > %s", c.config.Keyspace, c.config.CqlshPath, file)
	_, err := c.agent.Run(host, cmd)
	if err != nil {
		return "", err
	}
	return file, nil
}

// Snapshot takes incremental or full snapshot.
func (c *Cassandra) Snapshot(host, ts string) ([]string, []string, error) {
	if c.config.Incremental {
		return c.SnapshotInc(host)
	}
	return c.SnapshotFull(host, ts)
}

// SnapshotFull takes a full snapshot.
func (c *Cassandra) SnapshotFull(host, ts string) ([]string, []string, error) {
	cmd := fmt.Sprintf("%s snapshot -t %s %s", c.config.Nodetool, ts, c.config.Keyspace)
	bytes, err := c.agent.Run(host, cmd)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error taking snapshot on host %s with output %s", host, bytes)
	}
	return c.snapshotFullFiles(host, ts)
}

// Get files in snapshot
func (c *Cassandra) snapshotFullFiles(host, ts string) ([]string, []string, error) {

	// download cassandra yaml files
	dataDirs, err := c.hostDataDirs(host)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting data dir from host")
	}

	var files []string
	var dirs []string
	for _, dataDir := range dataDirs {
		keyspaceDir := fmt.Sprintf("%s/%s/", dataDir, c.config.Keyspace)
		tables, err := c.agent.ListDirs(host, keyspaceDir)
		if err != nil {
			return nil, nil, err
		}

		for _, table := range tables {
			if table == "" {
				continue
			}
			snapshotDir := fmt.Sprintf("%s/snapshots/%s/", table, ts)
			f, err := c.agent.ListFiles(host, snapshotDir)
			if err != nil {
				continue
			}
			dirs = append(dirs, snapshotDir)
			for _, file := range f {
				if file == "" {
					continue
				}
				files = append(files, file)
			}
		}
	}
	return files, dirs, nil
}

// SnapshotInc takes an incremental backup.
func (c *Cassandra) SnapshotInc(host string) ([]string, []string, error) {
	cmd := fmt.Sprintf("%s flush  %s", c.config.Nodetool, c.config.Keyspace)
	bytes, err := c.agent.Run(host, cmd)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error running flush on host %s with output %s", host, bytes)
	}
	return c.snapshotIncFiles(host)
}

// Get files in snapshot
func (c *Cassandra) snapshotIncFiles(host string) ([]string, []string, error) {
	// download cassandra yaml files
	dataDirs, err := c.hostDataDirs(host)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting data directories from host")
	}

	var files []string
	var dirs []string
	for _, dataDir := range dataDirs {
		keyspaceDir := fmt.Sprintf("%s/%s/", dataDir, c.config.Keyspace)
		tables, err := c.agent.ListDirs(host, keyspaceDir)
		if err != nil {
			return nil, nil, err
		}

		for _, table := range tables {
			if table == "" {
				continue
			}
			snapshotDir := fmt.Sprintf("%s/backups/", table)
			f, err := c.agent.ListFiles(host, snapshotDir)
			if err != nil {
				continue
			}
			dirs = append(dirs, snapshotDir)
			for _, file := range f {
				if file == "" {
					continue
				}
				files = append(files, file)
			}
		}
	}
	return files, dirs, nil
}

// cassandraConf ...
type cassandraConf struct {
	DataDirs []string `yaml:"data_file_directories"`
}

// parse cassandra conf file to get cassandra data directories.
func (c *Cassandra) hostDataDirs(host string) ([]string, error) {
	data, err := c.hostCassandraYaml(host)
	if err != nil {
		glog.Errorf("error reading host %s yaml file :: %v", host, err)
		return nil, err
	}

	var conf cassandraConf
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		glog.Infof("error unmarshalling yaml file")
		return nil, err
	}

	if conf.DataDirs == nil {
		return nil, fmt.Errorf("data file directories not specified in cassandra yaml file")
	}
	return conf.DataDirs, nil
}

// read cassandra conf file from remote cassandra host.
func (c *Cassandra) hostCassandraYaml(host string) ([]byte, error) {
	return c.agent.Run(host, fmt.Sprintf("cat %s/cassandra.yaml", c.config.CassandraConf))
}

func (c *Cassandra) deleteSnapshot(host string, dirs []string) error {
	glog.Infof("deleting local snapshot files...")
	for _, dir := range dirs {
		if len(dir) < 10 {
			glog.Errorf("something fishy, trying to delete dir: %s", dir)
			os.Exit(1)
		}
		c.agent.Run(host, fmt.Sprintf("rm -rf %s", dir))
	}
	return nil
}

// sstableloader loads sstable files in a directory to given cassandra cluster.
func (c *Cassandra) sstableload(target string, dirs map[string]bool) error {
	hosts := c.Hosts()
	for dir := range dirs {
		var err error
		for _, host := range hosts {
			cmd := fmt.Sprintf("%s --nodes %s -v %s", c.config.Sstableloader, host, dir)
			out, err := c.agent.Run(target, cmd)
			glog.V(2).Infof("sstableloader output: %s", out)
			if err == nil {
				glog.V(2).Infof("sstableloader passed")
				break
			}
			glog.V(2).Infof("sstableloader failed")
		}
		if err != nil {
			return errors.Wrap(err, "error running sstableloader")
		}
	}
	return nil
}
