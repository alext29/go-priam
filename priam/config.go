package priam

import (
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/user"
	"path"
)

// Config holds priam configuration parameters.
type Config struct {
	AwsAccessKey       string `yaml:"aws-access-key"`
	AwsBasePath        string `yaml:"aws-base-path"`
	AwsBucket          string `yaml:"aws-bucket"`
	AwsRegion          string `yaml:"aws-region"`
	AwsSecretKey       string `yaml:"aws-secret-key"`
	CassandraClasspath string `yaml:"cassandra-classpath"`
	CassandraConf      string `yaml:"cassandra-conf"`
	CqlshPath          string `yaml:"cqlsh-path"`
	Host               string
	Incremental        bool
	Keyspace           string
	Nodetool           string
	TempDir            string `yaml:"temp-dir"`
	PrivateKey         string `yaml:"private-key"`
	Snapshot           string
	Sstableloader      string
	User               string
}

// NewConfig returns priam configuration. It starts with the default config,
// superseeding these by parameters in config file, and finally
// superseeding them with command line flags.
func NewConfig() (*Config, error) {

	// get default config
	config, err := DefaultConfig()
	if err != nil {
		return nil, errors.Wrap(err, "error getting default config")
	}

	// parse config file
	if err := config.parseFile(configFile()); err != nil {
		return nil, errors.Wrapf(err, "error parsing config file %s", configFile())
	}

	// parse command line flags
	if err := config.parseFlags(); err != nil {
		return nil, errors.Wrap(err, "error parsing command line flags")
	}

	return config, nil
}

// DefaultConfig provides a starting point config for priam.
func DefaultConfig() (*Config, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "error getting current user")
	}
	return &Config{
		AwsBasePath:        "go-priam-test",
		AwsRegion:          "us-east-1",
		CassandraClasspath: "/usr/share/cassandra",
		CassandraConf:      "/etc/cassandra",
		CqlshPath:          "/usr/local/bin/cqlsh",
		Nodetool:           "/usr/bin/nodetool",
		PrivateKey:         path.Join(usr.HomeDir, ".ssh", "id_rsa"),
		Sstableloader:      "/usr/bin/sstableloader",
		TempDir:            "/tmp/go-priam/restore",
		User:               usr.Username,
	}, nil
}

// configFile returns path to priam config file.
func configFile() string {

	// use environment variable if set
	confFile := os.Getenv("PRIAM_CONF")
	if confFile != "" {
		return confFile
	}

	// else check home directory
	usr, err := user.Current()
	if err != nil {
		return ""
	}
	return path.Join(usr.HomeDir, ".priam.conf")
}

// parseFile parses priam config file. These may be overriden via
// command line flags.
func (c *Config) parseFile(confFile string) error {
	if confFile == "" {
		return nil
	}

	if _, err := os.Stat(confFile); os.IsNotExist(err) {
		return nil
	}

	fmt.Printf("reading conf file: %s\n", confFile)
	bytes, err := ioutil.ReadFile(confFile)
	if err != nil {
		return errors.Wrapf(err, "error reading conf file %s", confFile)
	}

	err = yaml.Unmarshal(bytes, c)
	if err != nil {
		return errors.Wrapf(err, "error unmarshaling conf file %s", confFile)
	}
	return nil
}

// parseFlags from command line.
func (c *Config) parseFlags() error {
	flag.BoolVar(&c.Incremental, "incremental", c.Incremental, "take incremental backup")
	flag.StringVar(&c.AwsAccessKey, "aws-access-key", c.AwsAccessKey, "AWS Access Key ID to access S3")
	flag.StringVar(&c.AwsBasePath, "aws-base-path", c.AwsBasePath, "base path to copy/restore files from S3")
	flag.StringVar(&c.AwsBucket, "aws-bucket", c.AwsBucket, "bucket name to store backups")
	flag.StringVar(&c.AwsRegion, "aws-region", c.AwsRegion, "region of s3 account")
	flag.StringVar(&c.AwsSecretKey, "aws-secret-key", c.AwsSecretKey, "AWS Secret Access key to access S3")
	flag.StringVar(&c.CassandraClasspath, "cassandra-classpath", c.CassandraClasspath, "directory where cassandra classfiles are placed")
	flag.StringVar(&c.CassandraConf, "cassandra-conf", c.CassandraConf, "directory where cassandra conf files are placed")
	flag.StringVar(&c.CqlshPath, "cqlsh-path", c.CqlshPath, "path to cqlsh")
	flag.StringVar(&c.Host, "host", c.Host, "ip address of any one of the cassandra hosts")
	flag.StringVar(&c.Keyspace, "keyspace", c.Keyspace, "cassandra keyspace to backup")
	flag.StringVar(&c.Nodetool, "nodetool-path", c.Nodetool, "path to nodetool on the cassandra host")
	flag.StringVar(&c.PrivateKey, "private-key", c.PrivateKey, "path to private key used for password less ssh")
	flag.StringVar(&c.Snapshot, "snapshot", c.Snapshot, "restore to this timestamp")
	flag.StringVar(&c.Sstableloader, "sstableloader", c.Sstableloader, "path to sstableloader on cassandra hosts")
	flag.StringVar(&c.TempDir, "temp-dir", c.TempDir, "temporary directory to download files to")
	flag.StringVar(&c.User, "user", c.User, "usename for password less ssh to cassandra host")

	flag.Parse()
	return c.validateConfig()
}

// validateConfig checks if all required parameters are provided.
func (c *Config) validateConfig() error {
	switch {
	case c.AwsAccessKey == "":
		return fmt.Errorf("please provide AWS Access Key ID (aws-access-key)")
	case c.AwsSecretKey == "":
		return fmt.Errorf("please provide AWS Secret Access key (aws-secret-key)")
	case c.AwsBucket == "":
		return fmt.Errorf("please provide AWS S3 bucket name (aws-bucket)")
	case c.PrivateKey == "":
		return fmt.Errorf("path to private key for passwordless ssh to cassandra hosts (private-key)")
	case c.Nodetool == "":
		return fmt.Errorf("path to nodetool not provided (nodetool-path)")
	case c.CassandraClasspath == "":
		return fmt.Errorf("path to casandra jarfiles not provided (cassandra-classpath)")
	case c.CassandraConf == "":
		return fmt.Errorf("path to casandra conf not provided (cassandra-conf)")
	case c.Host == "":
		return fmt.Errorf("please provide ip address of any cassandra node (host)")
	case c.User == "":
		return fmt.Errorf("please provide username to use for passwordless ssh (user)")
	case c.Sstableloader == "":
		return fmt.Errorf("please provide path to sstableloader executable on cassandra host (sstableloader)")
	}
	return nil
}

// String returns config in json string representation
func (c *Config) String() string {
	str := fmt.Sprintf("\n{")
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "aws-access-key", c.AwsAccessKey)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "aws-base-path", c.AwsBasePath)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "aws-bucket", c.AwsBucket)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "aws-region", c.AwsRegion)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "aws-secret-key", c.AwsSecretKey)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "cassandra-classpath", c.CassandraClasspath)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "cassandra-conf", c.CassandraConf)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "cqlsh-path", c.CqlshPath)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "host", c.Host)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "keyspace", c.Keyspace)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "nodetool", c.Nodetool)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "private-key", c.PrivateKey)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "snapshot", c.Snapshot)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "sstableloader", c.Sstableloader)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "temp-dir", c.TempDir)
	str = fmt.Sprintf("%s\n\t\"%s\": \"%s\",", str, "user", c.User)
	str = fmt.Sprintf("%s\n}\n", str[:len(str)-1])
	return str
}
