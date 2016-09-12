package prium

import (
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"os/user"
	"path"
)

// Config holds prium configuration parameters.
type Config struct {
	env           string
	host          string
	user          string
	incremental   bool
	prefix        string
	snapshot      string
	privateKey    string
	nodetool      string
	cassandraConf string
	keyspace      string
	awsBucket     string
	awsRegion     string
	awsSecretKey  string
	awsAccessKey  string
	sstableloader string
}

// NewConfig returns prium configuration. It starts with the default config,
// superseeding these by parameters in config file, and finally
// superseeding them with command line flags.
func NewConfig() (*Config, error) {

	// get default config
	config, err := defaultConfig()
	if err != nil {
		return nil, errors.Wrap(err, "error getting default config")
	}

	// TODO: parse config file if exists

	// parse command line flags
	if err := config.parseFlags(); err != nil {
		return nil, errors.Wrap(err, "error parsing command line flags")
	}

	return config, nil
}

func defaultConfig() (*Config, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "error getting current user")
	}
	return &Config{
		env:           "go-prium-test",
		prefix:        "/tmp/go-prium/restore",
		nodetool:      "/usr/bin/nodetool",
		sstableloader: "/usr/bin/sstableloader",
		cassandraConf: "/etc/cassandra",
		awsRegion:     "us-east-1",
		user:          usr.Username,
		privateKey:    path.Join(usr.HomeDir, ".ssh", "id_rsa"),
	}, nil
}

// parseFlags from command line.
func (c *Config) parseFlags() error {
	flag.StringVar(&c.env, "environment", c.env, "unique identifier for this cassandra cluster")
	flag.StringVar(&c.host, "host", c.host, "ip address of any one of the cassandra hosts")
	flag.StringVar(&c.user, "user", c.user, "usename for password less ssh to cassandra host")
	flag.StringVar(&c.keyspace, "keyspace", c.keyspace, "cassandra keyspace to backup")
	flag.StringVar(&c.nodetool, "nodetool-path", c.nodetool, "path to nodetool on the cassandra host")
	flag.StringVar(&c.privateKey, "private-key", c.privateKey, "path to private key used for password less ssh")
	flag.BoolVar(&c.incremental, "incremental", c.incremental, "take incremental backup")
	flag.StringVar(&c.cassandraConf, "cassandra-conf", c.cassandraConf, "directory where cassandra conf files are placed")
	flag.StringVar(&c.snapshot, "snapshot", c.snapshot, "restore to this timestamp")
	flag.StringVar(&c.prefix, "temp-dir", c.prefix, "temp directory to download files to")
	flag.StringVar(&c.awsRegion, "aws-region", c.awsRegion, "region of s3 account")
	flag.StringVar(&c.awsBucket, "aws-bucket", c.awsBucket, "bucket name to store backups")
	flag.StringVar(&c.awsSecretKey, "aws-secret-key", c.awsSecretKey, "AWS Secret Access key to access S3")
	flag.StringVar(&c.awsAccessKey, "aws-access-key", c.awsAccessKey, "AWS Access Key ID to access S3")
	flag.StringVar(&c.sstableloader, "sstableloader", c.sstableloader, "path to sstableloader on cassandra hosts")

	flag.Parse()
	return c.validateConfig()
}

// validateConfig has all the required parameters defined.
func (c *Config) validateConfig() error {
	switch {
	case c.awsAccessKey == "":
		return fmt.Errorf("please provide AWS Access Key ID via '-aws-access-key' commandline flag")
	case c.awsSecretKey == "":
		return fmt.Errorf("please provide AWS Secret Access key via '-aws-secret-key' commandline flag")
	case c.privateKey == "":
		return fmt.Errorf("path to private key for password less ssh to cassandra hosts")
	case c.nodetool == "":
		return fmt.Errorf("path to nodetool not provided")
	case c.cassandraConf == "":
		return fmt.Errorf("path to casandra conf not provided")
	case c.host == "":
		return fmt.Errorf("please provide ip address of any cassandra node")
	case c.user == "":
		return fmt.Errorf("please provide username to use for passwordless ssh")
	case c.sstableloader == "":
		return fmt.Errorf("please provide path to sstableloader executable on cassandra host")
	}
	return nil
}
