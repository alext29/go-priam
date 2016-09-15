package main

import (
	"flag"
	"fmt"
	"github.com/alext29/go-prium/prium"
	"github.com/golang/glog"
	"os"
)

func main() {

	// get configuration file
	config, err := prium.NewConfig()
	if err != nil {
		glog.Error(err)
		printUsage()
		return
	}
	glog.Infof("prium config %s", config)

	// make sure we have a valid command
	if len(flag.Args()) == 0 {
		glog.Error("please provide valid command")
		printUsage()
		os.Exit(1)
	}

	// create prium object
	p := prium.New(config)

	// parse and run command
	cmd := flag.Arg(0)
	switch cmd {
	case "backup":
		if err := p.Backup(); err != nil {
			glog.Error(err)
			os.Exit(1)
		}
		glog.Infof("backup completed")
	case "restore":
		if err := p.Restore(); err != nil {
			glog.Error(err)
			os.Exit(1)
		}
		glog.Infof("restore completed")
	case "history":
		if err := p.History(); err != nil {
			glog.Error(err)
			os.Exit(1)
		}
	default:
		glog.Errorf("unrecognized command '%s'", cmd)
		printUsage()
		os.Exit(1)
	}

}

func printUsage() {
	fmt.Println(`
USAGE: go-prium [OPTIONS] COMMAND

COMMAND

	backup          Backup cassandra DB to AWS S3 bucket.
	restore         Restore from a previous backup.
	history         Shows tree of all backups, including incremental backups.

OPTIONS

	-incremental            Switch to indicate incremental backup.
	-aws-access-key         AWS Access Key ID to access S3.
	-aws-base-path          Base path to copy/restore files from S3.
	-aws-bucket             S3 bucket name to store backups.
	-aws-region             Region of S3 account.
	-aws-secret-key         AWS Secret Access key to access S3.
	-cassandra-conf         Directory where cassandra conf files are placed.
	-cqlsh-path             Path fo cqlsh.
	-host                   IP address of any one of the cassandra nodes.
	-keyspace               Cassandra keyspace to backup.
	-nodetool-path          Path to nodetool on the cassandra host.
	-private-key            Path to private key used for password less ssh.
	-snapshot               Restore to this timestamp.
	-sstableloader          Path to sstableloader on cassandra hosts.
	-temp-dir               Temporary directory to download files to.
	-user                   Usename for password less ssh to cassandra host.
`)
}
