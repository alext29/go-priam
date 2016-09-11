package prium

import (
	"flag"
	"fmt"
)

// Config does something
type Config struct {
	env           *string
	host          *string
	user          *string
	incremental   *bool
	prefix        *string
	snapshot      *string
	privateKey    *string
	nodetool      *string
	cassandraConf *string
	keyspace      *string
	awsBucket     *string
	awsRegion     *string
	awsSecretKey  *string
	awsAccessKey  *string
	sstableloader *string
}

// NewConfig ...
func NewConfig() *Config {
	return &Config{}
}

// ParseFlags ...
func (c *Config) ParseFlags() error {
	c.env = flag.String("environment", "test", "unique identifier for this cassandra cluster")
	c.host = flag.String("host", "", "ip address of any one of the cassandra hosts")
	c.user = flag.String("user", "", "usename for password less ssh to cassandra host")
	c.keyspace = flag.String("keyspace", "sky_ks", "cassandra keyspace to backup")
	c.nodetool = flag.String("nodetool-path", "", "path to nodetool on the cassandra host")
	c.privateKey = flag.String("private-key", "", "path to private key used for password less ssh")
	c.incremental = flag.Bool("incremental", false, "take incremental backup")
	c.cassandraConf = flag.String("cassandra-conf", "", "directory where cassandra conf files are placed")
	c.snapshot = flag.String("snapshot", "", "restore to this timestamp")
	c.prefix = flag.String("temp-dir", "/tmp/go-prium/restore", "temp directory to download files to")
	c.awsRegion = flag.String("aws-region", "us-east-1", "region of s3 account")
	c.awsBucket = flag.String("aws-bucket", "", "bucket name to store backups")
	c.awsSecretKey = flag.String("aws-secret-key", "", "AWS Secret Access key to access S3")
	c.awsAccessKey = flag.String("aws-access-key", "", "AWS Access Key ID to access S3")
	c.sstableloader = flag.String("sstableloader", "", "path to sstableloader on cassandra hosts")

	flag.Parse()
	return c.validateConfig()
}

func (c *Config) validateConfig() error {
	switch {
	case *c.awsAccessKey == "":
		return fmt.Errorf("please provide AWS Access Key ID via '-aws-access-key' commandline flag")
	case *c.awsSecretKey == "":
		return fmt.Errorf("please provide AWS Secret Access key via '-aws-secret-key' commandline flag")
	case *c.privateKey == "":
		return fmt.Errorf("path to private key for password less ssh to cassandra hosts")
	case *c.nodetool == "":
		return fmt.Errorf("path to nodetool not provided")
	case *c.cassandraConf == "":
		return fmt.Errorf("path to casandra conf not provided")
	case *c.host == "":
		return fmt.Errorf("please provide ip address of any cassandra node")
	case *c.user == "":
		return fmt.Errorf("please provide username to use for passwordless ssh")
	case *c.sstableloader == "":
		return fmt.Errorf("please provide path to sstableloader executable on cassandra host")
	}

	return nil
}
