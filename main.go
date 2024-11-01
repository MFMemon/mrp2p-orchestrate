package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	// "github.com/golang/glog"

	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/deployments"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/mrjob"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/vms"
)

var (
	mrInputLocalDir        = flag.String("i", "", "absolute local path of the input files for mapreduce job")
	mrRbScriptLocalDir     = flag.String("b", "", "absolute local path of the record boundary finder script for mapreduce job")
	mrMapScriptLocalDir    = flag.String("m", "", "absolute local path of the map function script for mapreduce job")
	mrReduceScriptLocalDir = flag.String("r", "", "absolute local path of the reduce function script for mapreduce job")
	mrReduceOutDirName     = flag.String("o", "", "directory name of the final output files")
	mrNumOfOutFiles        = flag.Int("n", 2, "number of final output files to be created")
	mrInputRemoteDir       = "mrInput"
	mapFuncRemoteDir       = "mrMapFunc"
	reduceFuncRemoteDir    = "mrReduceFunc"
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

	// err := validateArgs()
	// if err != nil {
	// 	slog.Error(err.Error())
	// 	return
	// }

	err := cleanUpDir("server", "client", "plugin", "config.json", "cc.json")
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

	err = vms.SpinUpVms(peers)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	err = deployments.FSCreate(peers)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	time.Sleep(time.Second * 60) // wait for file system to be initialized

	_, err = deployments.FSUpload(mrInputRemoteDir, *mrInputLocalDir)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	mapFuncPath, err := deployments.FSUpload(mapFuncRemoteDir, *mrMapScriptLocalDir)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	reduceFuncPath, err := deployments.FSUpload(reduceFuncRemoteDir, *mrReduceScriptLocalDir)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	err = deployments.MRNodesCreate(
		peers, *mrNumOfOutFiles,
		mrInputRemoteDir,
		filepath.Join(mapFuncRemoteDir, mapFuncPath[0]),
		filepath.Join(reduceFuncRemoteDir, reduceFuncPath[0]),
		"",
		*mrReduceOutDirName,
	)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	utils.Logger().Infof("Cluster deployed sucessfully.")

	err = mrjob.Start()
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	utils.Logger().Infof("Job completed.")

}
