package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"
	"os"
	//"io/ioutil"
	"regexp"
	"strings"
)

// Cassandra ...
type Cassandra struct {
	config      *Config
	user        string
	host        string
	privateKey  string
	nodetool    string
	confPath    string
	agent       *agent
	hostClients map[string]*ssh.Client
}

// NewCassandra ...
func NewCassandra(config *Config) *Cassandra {
	return &Cassandra{
		config:      config,
		user:        *config.user,
		host:        *config.host,
		privateKey:  *config.privateKey,
		nodetool:    *config.nodetool,
		confPath:    *config.cassandraConf,
		hostClients: make(map[string]*ssh.Client),
		agent:       newAgent(config),
	}
}

// Init ...
func (c *Cassandra) Init() error {
	glog.Warningf("cassandra init not implemented...\n")
	return nil
}

// Hosts returns slice of cassandra hosts
func (c *Cassandra) Hosts() []string {
	cmd := fmt.Sprintf("%s status", c.nodetool)
	bytes, err := c.agent.run(c.host, cmd)
	if err != nil {
		glog.Errorf("error running cmd '%s' on host '%s' :: %v", cmd, c.host, err)
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
			glog.Infof("got cassandra host: %s", slices[1])
			hosts = append(hosts, slices[1])
		}
	}
	return hosts
}

// Snapshot takes incremental or full snapshot.
func (c *Cassandra) Snapshot(host, ts, ks string) ([]string, []string, error) {
	if *c.config.incremental {
		return c.SnapshotInc(host, ks)
	}
	return c.SnapshotFull(host, ts, ks)
}

// SnapshotFull takes a full snapshot.
func (c *Cassandra) SnapshotFull(host, ts, ks string) ([]string, []string, error) {
	cmd := fmt.Sprintf("%s snapshot -t %s %s", c.nodetool, ts, ks)
	glog.V(2).Infof("snapshot command: %s", cmd)
	bytes, err := c.agent.run(host, cmd)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("error taking snapshot on host %s with output %s", host, bytes))
	}
	return c.snapshotFullFiles(host, ts, ks)
}

// Get files in snapshot
func (c *Cassandra) snapshotFullFiles(host, ts, ks string) ([]string, []string, error) {

	// download cassandra yaml files
	dataDirs, err := c.hostDataDirs(host)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting data directories from host")
	}

	var files []string
	var dirs []string
	for _, dataDir := range dataDirs {
		keyspaceDir := fmt.Sprintf("%s/%s/", dataDir, ks)
		tables, err := c.agent.listDirs(host, keyspaceDir)
		if err != nil {
			return nil, nil, err
		}

		for _, table := range tables {
			if table == "" {
				continue
			}
			//glog.Infof("got table %s", table)
			snapshotDir := fmt.Sprintf("%s/snapshots/%s/", table, ts)
			//glog.Infof("snapshot dir: %s", snapshotDir)
			f, err := c.agent.listFiles(host, snapshotDir)
			if err != nil {
				continue
			}
			//glog.Infof("reading dir: %s", snapshotDir)
			dirs = append(dirs, snapshotDir)
			for _, file := range f {
				if file == "" {
					continue
				}
				//glog.Infof("got file: %s", file)
				files = append(files, file)
			}
		}
	}
	return files, dirs, nil
}

// SnapshotInc takes an incremental backup.
func (c *Cassandra) SnapshotInc(host, ks string) ([]string, []string, error) {
	cmd := fmt.Sprintf("%s flush  %s", c.nodetool, ks)
	glog.V(2).Infof("snapshot command: %s", cmd)
	bytes, err := c.agent.run(host, cmd)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("error running flush on host %s with output %s", host, bytes))
	}
	return c.snapshotIncFiles(host, ks)
}

// Get files in snapshot
func (c *Cassandra) snapshotIncFiles(host, ks string) ([]string, []string, error) {
	// download cassandra yaml files
	dataDirs, err := c.hostDataDirs(host)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting data directories from host")
	}

	var files []string
	var dirs []string
	for _, dataDir := range dataDirs {
		keyspaceDir := fmt.Sprintf("%s/%s/", dataDir, ks)
		tables, err := c.agent.listDirs(host, keyspaceDir)
		if err != nil {
			return nil, nil, err
		}

		for _, table := range tables {
			if table == "" {
				continue
			}
			snapshotDir := fmt.Sprintf("%s/backups/", table)
			f, err := c.agent.listFiles(host, snapshotDir)
			if err != nil {
				continue
			}
			dirs = append(dirs, snapshotDir)
			for _, file := range f {
				if file == "" {
					continue
				}
				//glog.Infof("got file: %s", file)
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

func (c *Cassandra) hostCassandraYaml(host string) ([]byte, error) {
	return c.agent.run(host, fmt.Sprintf("cat %s/cassandra.yaml", c.confPath))
}

func (c *Cassandra) deleteSnapshot(host string, dirs []string) error {
	glog.Infof("deleting local snapshot files...")
	for _, dir := range dirs {
		if len(dir) < 10 {
			glog.Errorf("something fishy, trying to delete dir: %s", dir)
			os.Exit(1)
		}
		c.agent.run(host, fmt.Sprintf("rm -rf %s", dir))
	}
	return nil
}
