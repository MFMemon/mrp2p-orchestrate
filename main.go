package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	// "github.com/golang/glog"

	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/deployments"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/vms"
)

var (
	mrInputLocalDir        = flag.String("in", "", "absolute local path of the input files for mapreduce job")
	mrRbScriptLocalDir     = flag.String("rbs", "", "absolute local path of the record boundary finder script for mapreduce job")
	mrMapScriptLocalDir    = flag.String("ms", "", "absolute local path of the map function script for mapreduce job")
	mrReduceScriptLocalDir = flag.String("rs", "", "absolute local path of the reduce function script for mapreduce job")
	mrWorkerScaleFactor    = flag.Int("ws", 4, "number of worker nodes required for mapreduce job")
)

func cleanUpDir(paths ...string) error {
	for i := range paths {
		err := os.RemoveAll(paths[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func validateArgs() error {

	if flag.Lookup("in") == nil {
		return fmt.Errorf("mandatory flag %s missing, use -h to see the usage", "in")
	}

	if flag.Lookup("rbs") == nil {
		return fmt.Errorf("mandatory flag %s missing, use -h to see the usage", "rbs")
	}

	if flag.Lookup("ms") == nil {
		return fmt.Errorf("mandatory flag %s missing, use -h to see the usage", "ms")
	}

	if flag.Lookup("rs") == nil {
		return fmt.Errorf("mandatory flag %s missing, use -h to see the usage", "rs")
	}

	return nil
}

func main() {

	flag.Parse()
	// zaputils.Logger(), _ := zap.NewProduction()
	// defer zaputils.Logger().Sync()
	// utils.Logger() = zaputils.Logger().Sugar()

	// err := validateArgs()
	// if err != nil {
	// 	slog.Error(err.Error())
	// 	return
	// }

	err := cleanUpDir("server", "client", "plugin", "config.json")
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	_, err = abs.Init(nil)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	peers, err := vms.UpdateAvailablePeers()
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	utils.Logger().Infof("Total peers found in the network: %v", len(peers))

	peerWithLowestRam, err := vms.SpinUpVms(peers, 3, *mrWorkerScaleFactor)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	err = deployments.FSCreate(peers, peerWithLowestRam)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	time.Sleep(time.Second * 10) // wait for file system to be initialized

	err = deployments.FSUpload("mrInput", *mrInputLocalDir)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

}
