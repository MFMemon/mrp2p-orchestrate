package deployments

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	abs "github.com/Akilan1999/p2p-rendering-computation/abstractions"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/vms"
)

type DeployedNodes map[string]*NodeConnInfo

type NodeConnInfo struct {
	ContainerId string   `json:"NodeId"`
	Ip          string   `json:"Ip"`
	Grpcport    vms.Port `json:"-"`
	Httpport    vms.Port `json:"Port"`
}

type ClusterConfig struct {
	MRWorkerIds      []string      `json:"MRWorkerIds"`
	MRMasterIds      []string      `json:"MRMasterIds"`
	MREtcdIds        []string      `json:"MREtcdIds"`
	FSMasterIds      []string      `json:"FSMasterIds"`
	FSVolumeIds      []string      `json:"FSVolumeIds"`
	FSFilerIds       []string      `json:"FSFilerIds"`
	MRP2PClusterInfo DeployedNodes `json:"MRP2PClusterInfo"`
}

var (
	NodesConnInfo DeployedNodes = make(DeployedNodes)
	CC            ClusterConfig
	ccPath        = "/tmp/cc.json"
)

func marshalClusterConfig() ([]byte, error) {
	// cc := ClusterConfig{
	// 	MRWorkerIds:      []string{"mrworker1", "mrworker2"},
	// 	MREtcdId:         []string{"mretcd"},
	// 	MRP2PClusterInfo: nodesConnInfo,
	// }
	CC.MRWorkerIds = append(CC.MRWorkerIds, "mrworker1")
	CC.MRWorkerIds = append(CC.MRWorkerIds, "mrworker2")
	CC.MRP2PClusterInfo = NodesConnInfo

	file, err := json.MarshalIndent(CC, "", "\t")
	return file, err
}

func FSUpload(fsDir string, localDir string) ([]string, error) {

	paths := make([]string, 0)

	fsFilerAddr := net.JoinHostPort(NodesConnInfo["fsfiler0"].Ip, NodesConnInfo["fsfiler0"].Httpport.HostPort)
	fsFilerUrl := fmt.Sprintf("http://%v/%v/", fsFilerAddr, fsDir)
	cli := http.Client{}

	err := filepath.WalkDir(localDir, func(path string, d fs.DirEntry, err error) error {

		// fmt.Println(path)
		if d.IsDir() {
			return nil
		}

		respBytes, httperr := abs.UploadFile(cli, fsFilerUrl, "filename", path)
		if httperr != nil {
			fmt.Println(httperr)
			return err
		}
		// fmt.Println(string(respBytes))

		utils.Logger().Infof("File upload succes: %v", string(respBytes))

		_, p := filepath.Split(path)
		paths = append(paths, p)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return paths, nil
}

func MRNodesCreate(peers []*vms.Peer, numOfOutFiles int, filepaths ...string) error {

	mrWorkerNodes, err := startMRWorkers(peers, numOfOutFiles, filepaths...)
	if err != nil {
		return err
	}

	for i := range mrWorkerNodes {
		workerNodeName := "mrworker_" + mrWorkerNodes[i].Httpport.ContainerPort + strconv.Itoa(i)
		NodesConnInfo[workerNodeName] = mrWorkerNodes[i]
		CC.MRWorkerIds = append(CC.MRWorkerIds, workerNodeName)
	}

	b, err := marshalClusterConfig()
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}

	err = os.WriteFile(ccPath, b, 0777)
	if err != nil {
		utils.Logger().Fatal(err.Error())
	}
	utils.Logger().Infof("cluster configuration file written successfully")

	mrMasterNodes, err := startMRMasters(peers, numOfOutFiles, ccPath, filepaths[0])
	if err != nil {
		return err
	}

	for i := range mrMasterNodes {
		masterNodeName := "mrmaster" + strconv.Itoa(i)
		NodesConnInfo[masterNodeName] = mrMasterNodes[i]
		CC.MRMasterIds = append(CC.MRMasterIds, masterNodeName)
	}

	// mrMasterNode, err := startMRMaster(peerWithLowestRam.MasterContainer, numOfOutFiles, ccPath, filepaths[0])
	// if err != nil {
	// 	return err
	// }
	// nodesConnInfo["mrmaster"] = mrMasterNode

	return nil
}

func FSCreate(peers []*vms.Peer) error {

	// fsMasterNodes, err := startFsMasterNodes(peers)
	// if err != nil {
	// 	return err
	// }
	// for i := range fsMasterNodes {
	// 	masterName := "fsmaster" + strconv.Itoa(i)
	// 	NodesConnInfo[masterName] = fsMasterNodes[i]
	// 	CC.FSMasterIds = append(CC.FSMasterIds, masterName)
	// }

	fsMasterNode, err := startFsMasterNode(peers[0].Containers[0])
	if err != nil {
		return err
	}
	masterName := "fsmaster"
	NodesConnInfo[masterName] = fsMasterNode
	CC.FSMasterIds = append(CC.FSMasterIds, masterName)

	// fsVolumeNodes, err := startFsVolumeNodes(peers, CC.FSMasterIds)
	// if err != nil {
	// 	return err
	// }
	// for i := range fsVolumeNodes {
	// 	volumeName := "fsvolume" + strconv.Itoa(i)
	// 	NodesConnInfo[volumeName] = fsVolumeNodes[i]
	// 	CC.FSVolumeIds = append(CC.FSVolumeIds, volumeName)
	// }

	fsVolumeNode, err := startFsVolumeNode(peers[0].Containers[0], NodesConnInfo["fsmaster"])
	if err != nil {
		return err
	}
	volumeName := "fsvolume"
	NodesConnInfo[volumeName] = fsVolumeNode
	CC.FSVolumeIds = append(CC.FSVolumeIds, volumeName)

	// fsFilerNodes, err := startFsFilerNodes(peers, CC.FSMasterIds)
	// if err != nil {
	// 	return err
	// }
	// for i := range fsFilerNodes {
	// 	filerName := "fsfiler" + strconv.Itoa(i)
	// 	NodesConnInfo[filerName] = fsFilerNodes[i]
	// 	CC.FSFilerIds = append(CC.FSFilerIds, filerName)
	// }

	mrEtcdNodes, err := startMrEtcdNodes(peers)
	if err != nil {
		return err
	}
	for i := range mrEtcdNodes {
		etcdNodeName := "mretcd" + strconv.Itoa(i)
		NodesConnInfo[etcdNodeName] = mrEtcdNodes[i]
		CC.MREtcdIds = append(CC.MREtcdIds, etcdNodeName)
	}

	return nil
}

// func startFsMasterNodes(connectedPeers []*vms.Peer) ([]*NodeConnInfo, error) {

// 	var masterNodes []*NodeConnInfo

// 	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsmaster")

// 	if err != nil {
// 		utils.Logger().Infof(err.Error())
// 	}

// 	fsMasterPeerPorts := make([][]*vms.Port, 0)
// 	fsMasterPeerIps := make([]string, 0)

// 	for i, _ := range connectedPeers {
// 		peer := connectedPeers[i]
// 		for j, _ := range peer.Containers {
// 			con := peer.Containers[j]
// 			conFSMasterPorts := make([]*vms.Port, 0)

// 			for k := range con.Ports {
// 				if con.Ports[k].Name == "FSMasterGrpcPort" || con.Ports[k].Name == "FSMasterHttpPort" {
// 					conFSMasterPorts = append(conFSMasterPorts, con.Ports[k])
// 				}
// 			}

// 			fsMasterPeerPorts = append(
// 				fsMasterPeerPorts,
// 				conFSMasterPorts,
// 			)

// 			fsMasterPeerIps = append(fsMasterPeerIps, con.Ip)
// 		}
// 	}

// 	for i, _ := range connectedPeers {
// 		peer := connectedPeers[i]
// 		for j, _ := range peer.Containers {
// 			con := peer.Containers[j]
// 			thisPeerAddrIdx := i*len(peer.Containers) + j

// 			targetNodePorts := fsMasterPeerPorts[thisPeerAddrIdx]

// 			targetNodePeerPorts := make([][]*vms.Port, 0)
// 			targetNodePeerPorts = append(targetNodePeerPorts, fsMasterPeerPorts[0:thisPeerAddrIdx]...)
// 			targetNodePeerPorts = append(targetNodePeerPorts, fsMasterPeerPorts[thisPeerAddrIdx+1:]...)

// 			targetNodePeerIps := make([]string, 0)
// 			targetNodePeerIps = append(targetNodePeerIps, fsMasterPeerIps[0:thisPeerAddrIdx]...)
// 			targetNodePeerIps = append(targetNodePeerIps, fsMasterPeerIps[thisPeerAddrIdx+1:]...)

// 			node, err := startFsMasterNode(con, plugin, targetNodePorts, targetNodePeerPorts, targetNodePeerIps)
// 			if err != nil {
// 				return nil, err
// 			}

// 			masterNodes = append(masterNodes, node)
// 		}
// 	}
// 	return masterNodes, nil
// }

func startFsMasterNode(con *vms.ContainerInfo) (*NodeConnInfo, error) {
	// err := abs.Pul
	requiredPorts := make([]*vms.Port, 0)

	for i := range con.Ports {
		if con.Ports[i].Name == "FSMasterHttpPort" {
			requiredPorts = append(requiredPorts, con.Ports[i])
			con.Ports[i].Taken = true
		}
		if con.Ports[i].Name == "FSMasterGrpcPort" {
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

	nodeConn.Httpport.ContainerPort = requiredPorts[0].ContainerPort
	nodeConn.Httpport.HostPort = requiredPorts[0].HostPort
	nodeConn.Grpcport.ContainerPort = requiredPorts[1].ContainerPort
	nodeConn.Grpcport.HostPort = requiredPorts[1].HostPort

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

// func startFsVolumeNodes(connectedPeers []*vms.Peer, fsMasterPeerNames []string) ([]*NodeConnInfo, error) {
// 	// err := abs.Pul
// 	var volumeNodes []*NodeConnInfo

// 	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsvolume")

// 	if err != nil {
// 		utils.Logger().Infof(err.Error())
// 	}

// 	for i, _ := range connectedPeers {
// 		peer := connectedPeers[i]
// 		for j, _ := range peer.Containers {
// 			con := peer.Containers[j]
// 			node, err := startFsVolumeNode(con, fsMasterPeerNames, plugin)
// 			if err != nil {
// 				return nil, err
// 			}

// 			volumeNodes = append(volumeNodes, node)
// 		}
// 	}
// 	return volumeNodes, nil
// }

func startFsVolumeNode(con *vms.ContainerInfo, fsMasterConnInfo *NodeConnInfo) (*NodeConnInfo, error) {
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

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsvolume")

	pluginArgs := []string{nodeConn.Httpport.ContainerPort,
		fmt.Sprintf("%v:%v", fsMasterIp, fsMasterPort-10000)}

	err = abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("File system volume server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startFsFilerNodes(connectedPeers []*vms.Peer, fsMasterPeerNames []string) ([]*NodeConnInfo, error) {
	// err := abs.Pul
	var filerNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pfsfiler")

	if err != nil {
		utils.Logger().Infof(err.Error())
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.Containers {
			con := peer.Containers[j]
			node, err := startFsFilerNode(con, fsMasterPeerNames, plugin)
			if err != nil {
				return nil, err
			}

			filerNodes = append(filerNodes, node)
		}
	}
	return filerNodes, nil
}

func startFsFilerNode(con *vms.ContainerInfo, fsMasterPeerNames []string, plugin string) (*NodeConnInfo, error) {
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

	fsMasterPeerAddrs := make([]string, 0)

	for i := range fsMasterPeerNames {
		masterName := fsMasterPeerNames[i]
		masterIp := NodesConnInfo[masterName].Ip
		masterPort, _ := strconv.Atoi(NodesConnInfo[masterName].Grpcport.HostPort)

		fsMasterPeerAddrs = append(
			fsMasterPeerAddrs,
			fmt.Sprintf("%v:%v", masterIp, masterPort-10000),
		)
	}

	pluginArgs := []string{nodeConn.Httpport.ContainerPort, strings.Join(fsMasterPeerAddrs, ",")}

	err := abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("File system filer server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startMrEtcdNodes(connectedPeers []*vms.Peer) ([]*NodeConnInfo, error) {

	var etcdNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2petcd")

	if err != nil {
		utils.Logger().Infof(err.Error())
	}

	etcdPeers := make([]string, 0)
	etcdClients := make([]string, 0)

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.Containers {
			con := peer.Containers[j]

			for k := range con.Ports {
				if con.Ports[k].Name == "EtcdServerPort" {
					name := "etcdNode" + strconv.Itoa(i) + strconv.Itoa(j)
					ip := con.Ip
					port := con.Ports[k].HostPort
					etcdPeers = append(
						etcdPeers,
						fmt.Sprintf("%v=http://%v:%v", name, ip, port),
					)
				}
				if con.Ports[k].Name == "EtcdClientPort" {
					ip := con.Ip
					port := con.Ports[k].HostPort
					etcdClients = append(
						etcdClients,
						fmt.Sprintf("http://%v:%v", ip, port),
					)
				}
			}
		}
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.Containers {
			con := peer.Containers[j]

			node, err := startMrEtcdNode(
				con, plugin, etcdPeers,
				etcdPeers[i*len(peer.Containers)+j],
				etcdClients[i*len(peer.Containers)+j],
			)
			if err != nil {
				return nil, err
			}

			etcdNodes = append(etcdNodes, node)
		}
	}
	return etcdNodes, nil
}

func startMrEtcdNode(con *vms.ContainerInfo, plugin string,
	peerNamesAndAdvertiseUrls []string, peerNameAndAdvertiseUrl string, clientAdvertiseUrl string) (*NodeConnInfo, error) {

	var requiredPort *vms.Port

	clientLocalListenIp := "127.0.0.1"

	for i, _ := range con.Ports {
		if con.Ports[i].Name == "EtcdClientPort" {
			requiredPort = con.Ports[i]
			con.Ports[i].Taken = true
			break
		}
	}

	nodeConn := new(NodeConnInfo)
	nodeConn.ContainerId = con.Id
	nodeConn.Ip = clientLocalListenIp
	nodeConn.Httpport.ContainerPort = requiredPort.ContainerPort
	nodeConn.Httpport.HostPort = requiredPort.HostPort

	peerName := strings.Split(peerNameAndAdvertiseUrl, "=")[0]
	peerAdvertiseUrl := strings.Split(peerNameAndAdvertiseUrl, "=")[1]

	pluginArgs := []string{
		peerName, peerAdvertiseUrl, clientLocalListenIp,
		clientAdvertiseUrl, strings.Join(peerNamesAndAdvertiseUrls, ","),
	}

	err := abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("etcd client started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startMRWorkers(
	connectedPeers []*vms.Peer, numOfOutputFiles int, filePaths ...string) ([]*NodeConnInfo, error) {

	var workerNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pmrworker")

	if err != nil {
		utils.Logger().Infof(err.Error())
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.Containers {
			con := peer.Containers[j]
			for k := range []int{1, 2} {
				node, err := startMRWorker(con, k, numOfOutputFiles, plugin, filePaths...)
				if err != nil {
					return nil, err
				}
				workerNodes = append(workerNodes, node)
			}

		}
	}
	return workerNodes, nil
}

func startMRWorker(con *vms.ContainerInfo, id int, numOfOutputFiles int, plugin string, filepaths ...string) (*NodeConnInfo, error) {
	var requiredPort *vms.Port

	for i, _ := range con.Ports {
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

	fsFilerNode := NodesConnInfo[CC.FSFilerIds[0]]

	pluginArgs := []string{strconv.Itoa(id), nodeConn.Httpport.ContainerPort,
		fmt.Sprintf("%v:%v", fsFilerNode.Ip, fsFilerNode.Httpport.HostPort),
		nodeConn.ContainerId,
		filepaths[1],
		filepaths[2],
		strconv.Itoa(numOfOutputFiles),
		filepaths[4],
	}

	err := abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("Mapreduce worker node started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}

func startMRMasters(
	connectedPeers []*vms.Peer, numOfOutputFiles int, cconf string, inputDir string) ([]*NodeConnInfo, error) {

	var masterNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pmrmaster")

	if err != nil {
		utils.Logger().Infof(err.Error())
		return nil, err
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.Containers {
			con := peer.Containers[j]
			node, err := startMRMaster(
				con, plugin, CC.MREtcdIds[i*len(peer.Containers)+j],
				numOfOutputFiles, cconf, inputDir, strconv.Itoa(i*len(peer.Containers)+j),
			)

			if err != nil {
				return nil, err
			}
			masterNodes = append(masterNodes, node)

		}
	}
	return masterNodes, nil
}

func startMRMaster(con *vms.ContainerInfo, plugin string, etcdNodeName string, numOfReducers int,
	cconf string, inputDir string, id string) (*NodeConnInfo, error) {

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

	pluginArgs := []string{
		nodeConn.Httpport.ContainerPort, inputDir, cconf, strconv.Itoa(numOfReducers),
		fmt.Sprintf(
			"http://%v:%v", NodesConnInfo[etcdNodeName].Ip,
			NodesConnInfo[etcdNodeName].Httpport.ContainerPort,
		),
		id,
	}

	err := abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("Mapreduce master node started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}
