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

	// make sure we have a valid command
	if len(flag.Args()) == 0 {
		glog.Error("please provide valid command")
		printUsage()
		os.Exit(1)
	}

	// create prium object
	p := prium.New(config)
	p.Init()

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
	default:
		glog.Errorf("unrecognized command '%s'", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("USAGE")
}
