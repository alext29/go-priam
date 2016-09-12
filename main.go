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

	p := prium.New(config)
	p.Init()

	// get command
	cmd := flag.Arg(0)
	glog.Infof("got command: %s\n", cmd)

	switch cmd {
	case "backup":
		if err := p.Backup(); err != nil {
			glog.Errorf("%v", err)
			os.Exit(1)
		}
		glog.Infof("backup completed")
	case "restore":
		if err := p.Restore(); err != nil {
			glog.Errorf("%v", err)
			os.Exit(1)
		}
		glog.Infof("restore completed")
	case "":
		glog.Errorf("did not get valid command")
		printUsage()
		os.Exit(1)
	default:
		glog.Errorf("unrecognized command '%s'", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("USAGE")
}
