package deployments

import (
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"

	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/vms"
)

type DeployedNodes map[string]*NodeConnInfo

type NodeConnInfo struct {
	ContainerId string
	Ip          string
	Grpcport    vms.Port
	Httpport    vms.Port
}

var nodesConnInfo DeployedNodes = make(DeployedNodes)

func FSUpload(fsDir string, localDir string) error {
	fsFilerAddr := net.JoinHostPort(nodesConnInfo["fsfiler"].Ip, nodesConnInfo["fsfiler"].Httpport.HostPort)
	fsFilerUrl := fmt.Sprintf("https://%v/%v", fsFilerAddr, fsDir)
	// postUrl, err := url.JoinPath(fsFilerAddr, fsDir)
	// if err != nil {
	// 	return err
	// }

	err := filepath.WalkDir(localDir, func(path string, d fs.DirEntry, err error) error {

		if d.IsDir() {
			return nil
		}

		_, httperr := http.PostForm(fsFilerUrl, url.Values{
			"filename": {path},
		})
		if httperr != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	// for i := range paths {
	// 	_, err := http.PostForm(postUrl, url.Values{
	// 		"filename": {paths[i]},
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func FSCreate(peers []*vms.Peer, peerWithLowestRam *vms.Peer) error {

	fsMasterNode, err := startFsMasterNode(peerWithLowestRam.MasterContainer)
	if err != nil {
		return err
	}
	nodesConnInfo["fsmaster"] = fsMasterNode

	etcdNode, err := startEtcdNode(peerWithLowestRam.MasterContainer)
	if err != nil {
		return err
	}
	nodesConnInfo["etcd"] = etcdNode

	fsVolumeNodes, err := startFsVolumeNodes(peers, nodesConnInfo["fsmaster"])
	if err != nil {
		return err
	}
	for i := range fsVolumeNodes {
		volumeName := "fsvolume" + strconv.Itoa(i)
		nodesConnInfo[volumeName] = fsVolumeNodes[i]
	}

	fsFilerNode, err := startFsFilerNode(peerWithLowestRam.MasterContainer, nodesConnInfo["fsmaster"])
	if err != nil {
		return err
	}
	nodesConnInfo["fsfiler"] = fsFilerNode

	return nil
}

func startFsMasterNode(con *vms.ContainerInfo) (*NodeConnInfo, error) {
	// err := abs.Pul
	requiredPorts := make([]*vms.Port, 0)

	for i := range con.Ports {
		if con.Ports[i].Name == "AutoGen Port" {
			requiredPorts = append(requiredPorts, con.Ports[i])
			con.Ports[i].Taken = true
		}
		if len(requiredPorts) == 2 {
			break
		}
	}

	nodeConn := new(NodeConnInfo)
	nodeConn.ContainerId = con.Id
	nodeConn.Ip = con.Ip

	nodeConn.Grpcport.ContainerPort = requiredPorts[0].ContainerPort
	nodeConn.Grpcport.HostPort = requiredPorts[0].HostPort
	nodeConn.Httpport.ContainerPort = requiredPorts[1].ContainerPort
	nodeConn.Httpport.HostPort = requiredPorts[1].HostPort

	pluginArgs := []string{nodeConn.Grpcport.ContainerPort, nodeConn.Httpport.ContainerPort}
	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsmaster")
	if err != nil {
		return nil, err
	}

	err = abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("File system master server started at %v, %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Grpcport.HostPort),
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startEtcdNode(con *vms.ContainerInfo) (*NodeConnInfo, error) {
	// err := abs.Pul
	var requiredPort *vms.Port

	for i, _ := range con.Ports {
		if con.Ports[i].Name == "EtcdPort" {
			requiredPort = con.Ports[i]
			con.Ports[i].Taken = true
			break
		}
	}

	nodeConn := new(NodeConnInfo)
	nodeConn.ContainerId = con.Id
	nodeConn.Ip = con.Ip
	nodeConn.Grpcport.ContainerPort = requiredPort.ContainerPort
	nodeConn.Grpcport.HostPort = requiredPort.HostPort

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2petcd")
	if err != nil {
		return nil, err
	}

	err = abs.ExecutePlugin(plugin, con.Id, nil)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("etcd server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Grpcport.HostPort),
	)

	return nodeConn, nil
}

func startFsVolumeNodes(connectedPeers []*vms.Peer, fsMasterConnInfo *NodeConnInfo) ([]*NodeConnInfo, error) {
	// err := abs.Pul
	var volumeNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsvolume")

	if err != nil {
		utils.Logger().Infof(err.Error())
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.OtherContainers {
			con := peer.OtherContainers[j]
			node, err := startFsVolumeNode(con, fsMasterConnInfo, plugin)
			if err != nil {
				return nil, err
			}

			volumeNodes = append(volumeNodes, node)
		}
	}
	return volumeNodes, nil
}

func startFsVolumeNode(con *vms.ContainerInfo, fsMasterConnInfo *NodeConnInfo, plugin string) (*NodeConnInfo, error) {
	var requiredPort *vms.Port

	for i, _ := range con.Ports {
		if con.Ports[i].Name == "AutoGen Port" {
			requiredPort = con.Ports[i]
			con.Ports[i].Taken = true
			break
		}
	}

	nodeConn := new(NodeConnInfo)
	nodeConn.ContainerId = con.Id
	nodeConn.Ip = con.Ip
	nodeConn.Httpport.ContainerPort = requiredPort.ContainerPort
	nodeConn.Httpport.HostPort = requiredPort.HostPort

	fsMasterIp := fsMasterConnInfo.Ip
	fsMasterPort, _ := strconv.Atoi(fsMasterConnInfo.Grpcport.HostPort)

	pluginArgs := []string{nodeConn.Httpport.ContainerPort,
		fmt.Sprintf("%v:%v", fsMasterIp, fsMasterPort-10000)}

	err := abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("File system volume server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startFsFilerNode(con *vms.ContainerInfo, fsMasterConnInfo *NodeConnInfo) (*NodeConnInfo, error) {
	var requiredPort *vms.Port

	for i := range con.Ports {
		if con.Ports[i].Name == "AutoGen Port" && !con.Ports[i].Taken {
			requiredPort = con.Ports[i]
			con.Ports[i].Taken = true
			break
		}
	}

	nodeConn := new(NodeConnInfo)
	nodeConn.ContainerId = con.Id
	nodeConn.Ip = con.Ip
	nodeConn.Httpport.ContainerPort = requiredPort.ContainerPort
	nodeConn.Httpport.HostPort = requiredPort.HostPort

	fsMasterIp := fsMasterConnInfo.Ip
	fsMasterPort, _ := strconv.Atoi(fsMasterConnInfo.Grpcport.HostPort)

	pluginArgs := []string{nodeConn.Httpport.ContainerPort,
		fmt.Sprintf("%v:%v", fsMasterIp, fsMasterPort-10000)}

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsfiler")
	if err != nil {
		return nil, err
	}

	err = abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("File system filer server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}
