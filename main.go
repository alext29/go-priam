package main

import (
	"flag"
	"fmt"
	"github.com/alext29/go-prium/prium"
	"github.com/golang/glog"
)

func main() {

	// get configuration file
	config := prium.NewConfig()
	if err := config.ParseFlags(); err != nil {
		glog.Errorf("error parsing flags :: %v\n", err)
		printUsage()
		return
	}

	if len(flag.Args()) == 0 {
		glog.Error("zero length of arguments")
		printUsage()
		return
	}

	// get cassandra object
	cassandra := prium.NewCassandra(config)
	if err := cassandra.Init(); err != nil {
		glog.Errorf("error initializing connection to cassandra :: %v\n", err)
		return
	}

	// get s3 object
	s3 := prium.NewS3(config)
	if err := s3.Init(); err != nil {
		glog.Errorf("error initializing s3 :: %v\n", err)
		return
	}

	// get command
	cmd := flag.Arg(0)
	glog.Infof("got command: %s\n", cmd)

	switch cmd {
	case "backup":
		if err := prium.Backup(config, cassandra, s3); err != nil {
			glog.Errorf("%v", err)
		} else {
			glog.Infof("backup completed")
		}
	case "restore":
		if err := prium.Restore(config, cassandra, s3); err != nil {
			glog.Errorf("%v", err)
		} else {
			glog.Infof("restore completed")
		}
	case "":
		glog.Errorf("did not get valid command")
	default:
		glog.Errorf("unrecognized command '%s'", cmd)
	}
}

func printUsage() {
	fmt.Println("USAGE")
}
