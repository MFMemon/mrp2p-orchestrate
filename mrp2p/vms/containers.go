package vms

import (
	"fmt"
	"strconv"

	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
)

type ContainerInfo struct {
	Id    string
	Ip    string
	Ports []*Port
}

type Port struct {
	Name          string `json:"-"`
	ContainerPort string `json:"LocalPort"`
	HostPort      string `json:"PublicPort"`
	Taken         bool   `json:"-"`
}

const (
	vmBaseName  = "mrp2p-docker"
	vmBaseImage = "python:3.12"
)

func spinUpContainer(targetPeer *Peer, numOfFsPorts int, numOfMrPorts int) error {

	targetPeerAddr := fmt.Sprintf("%v:%v", targetPeer.Ip, targetPeer.Port)
	con, err := abs.StartContainer(targetPeerAddr, numOfFsPorts+numOfMrPorts, vmBaseName, vmBaseImage)
	if err != nil {
		return err
	}

	conInfo := ContainerInfo{
		Id: con.ID,
		Ip: targetPeer.Ip,
	}

	for i := range con.Ports.PortSet {
		portInfo := con.Ports.PortSet[i]
		conInfo.Ports = append(conInfo.Ports, &Port{
			Name:          portInfo.PortName,
			ContainerPort: strconv.Itoa(portInfo.InternalPort),
			HostPort:      strconv.Itoa(portInfo.ExternalPort),
			Taken:         false,
		})
	}

	targetPeer.Containers = append(targetPeer.Containers, &conInfo)
	utils.Logger().Infof("Started container at peer %s in p2prc network", targetPeer.Name)

	return nil
}

func SpinUpVms(peers []*Peer) error {

	// var lowestAvailableRam uint64 = 98304.0 // 96 GB

	for i := range peers {
		peer := peers[i]
		err := spinUpContainer(peer, 4, 3) // spins up a container that will run file system master and mapreduce master
		if err != nil {
			return err
		}
		utils.Logger().Infof("Peer %v: %v", i, peer.Name)

		err = spinUpContainer(peer, 4, 3) // spins up a container that will run file system master and mapreduce master
		if err != nil {
			return err
		}

		err = spinUpContainer(peer, 4, 3) // spins up a container that will run file system master and mapreduce master
		if err != nil {
			return err
		}
	}

	return nil
}
