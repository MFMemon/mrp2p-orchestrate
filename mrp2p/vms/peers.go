package vms

import (
	"fmt"

	// "github.com/golang/glog"
	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
)

type Peer struct {
	Name       string
	Ip         string
	Port       string
	Containers []*ContainerInfo
	Resources  Specs
}

type Specs struct {
	Ram  uint64
	Disk uint64
}

func UpdateAvailablePeers() ([]*Peer, error) {

	var peers []*Peer

	err := abs.UpdateIPTable()
	if err != nil {
		return nil, err
	}

	availablePeerNodes, err := abs.ViewIPTable()
	if err != nil {
		return nil, err
	}

	for i, _ := range availablePeerNodes.IpAddress {
		node := availablePeerNodes.IpAddress[i]
		if node.EscapeImplementation == "FRP" {
			peerSpec, _ := abs.GetSpecs(node.Ipv4 + ":" + node.ServerPort)
			peer := Peer{
				Name: node.Name,
				Ip:   node.Ipv4,
				Port: node.ServerPort,
				Resources: Specs{
					Ram:  peerSpec.RAM,
					Disk: peerSpec.Disk,
				},
			}
			peers = append(peers, &peer)
		}
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("No peers found in the network.")
	}

	return peers, nil
}
