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
	Name          string
	ContainerPort string
	HostPort      string
	Taken         bool
}

func spinUpContainer(targetPeer *Peer, numOfFsPorts int, numOfMrPorts int, isMaster bool) error {

	targetPeerAddr := fmt.Sprintf("%v:%v", targetPeer.Ip, targetPeer.Port)
	con, err := abs.StartContainer(targetPeerAddr, numOfFsPorts+numOfMrPorts, "mrp2p-docker", "python:3.12")
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

	if isMaster {
		targetPeer.MasterContainer = &conInfo
	} else {
		targetPeer.OtherContainers = append(targetPeer.OtherContainers, &conInfo)
	}

	utils.Logger().Infof("Started container at peer %s in p2prc network", targetPeer.Name)

	return nil
}

func SpinUpVms(peers []*Peer, minimumContainersRequired int, mrWorkerScaleFactor int) (*Peer, error) {

	var peerWithLowestRam *Peer
	var lowestAvailableRam uint64 = 98304.0 // 96 GB

	for i := range peers {
		peer := peers[i]
		if peer.Resources.Ram < lowestAvailableRam {
			peerWithLowestRam = peers[i]
			lowestAvailableRam = peer.Resources.Ram
		}
		utils.Logger().Infof("Peer %v: %v", i, peer.Name)
	}

	totalContainersRunning := 0

	err := spinUpContainer(peerWithLowestRam, 3, 1, true) // spins up a container that will run file system master and mapreduce master
	if err != nil {
		return nil, err
	}

	totalContainersRunning += 1

	for totalContainersRunning < minimumContainersRequired {

		if len(peers) == 1 {
			err = spinUpContainer(peers[0], 1, mrWorkerScaleFactor, false) // spins up a container that will run file system volume and mapreduce worker
			if err != nil {
				return nil, err
			}

			totalContainersRunning += 1

		} else {

			for i := range peers {
				if peers[i].Name != peerWithLowestRam.Name {
					err = spinUpContainer(peers[i], 1, mrWorkerScaleFactor, false) // spins up a container that will run file system volume and mapreduce worker
					if err != nil {
						return nil, err
					}
					totalContainersRunning += 1
				}
			}
		}
	}
	return peerWithLowestRam, nil
}
