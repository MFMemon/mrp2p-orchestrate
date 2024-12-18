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
	MREtcdIds        []string      `json:"MREtcdIds"`
	FSMasterIds      []string      `json:"FSMasterIds"`
	FSVolumeIds      []string      `json:"FSVolumeIds"`
	FSFilerIds       []string      `json:"FSFilerIds"`
	MRP2PClusterInfo DeployedNodes `json:"MRP2PClusterInfo"`
}

var (
	nodesConnInfo DeployedNodes = make(DeployedNodes)
	cc            ClusterConfig
	ccPath        = "/tmp/cc.json"
)

func marshalClusterConfig() ([]byte, error) {
	// cc := ClusterConfig{
	// 	MRWorkerIds:      []string{"mrworker1", "mrworker2"},
	// 	MREtcdId:         []string{"mretcd"},
	// 	MRP2PClusterInfo: nodesConnInfo,
	// }
	cc.MRWorkerIds = append(cc.MRWorkerIds, "mrworker1")
	cc.MRWorkerIds = append(cc.MRWorkerIds, "mrworker2")
	cc.MRP2PClusterInfo = nodesConnInfo

	file, err := json.MarshalIndent(cc, "", "\t")
	return file, err
}

func FSUpload(fsDir string, localDir string) ([]string, error) {

	paths := make([]string, 0)

	fsFilerAddr := net.JoinHostPort(nodesConnInfo["fsfiler"].Ip, nodesConnInfo["fsfiler"].Httpport.HostPort)
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

func MRNodesCreate(peers []*vms.Peer, peerWithLowestRam *vms.Peer, numOfOutFiles int, filepaths ...string) error {

	mrWorkerNode, err := startMRWorkers(peers, numOfOutFiles, filepaths...)
	if err != nil {
		return err
	}

	for i := range mrWorkerNode {
		workerNodeName := "mrworker_" + mrWorkerNode[i].Httpport.ContainerPort + strconv.Itoa(i)
		nodesConnInfo[workerNodeName] = mrWorkerNode[i]
		cc.MRWorkerIds = append(cc.MRWorkerIds, workerNodeName)
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

	mrMasterNode, err := startMRMaster(peerWithLowestRam.MasterContainer, numOfOutFiles, ccPath, filepaths[0])
	if err != nil {
		return err
	}
	nodesConnInfo["mrmaster"] = mrMasterNode

	return nil
}

func FSCreate(peers []*vms.Peer, peerWithLowestRam *vms.Peer) error {

	// wg := new(sync.WaitGroup)
	// wg.Add(3)

	// errChan := make(chan error, 3)
	// defer close(errChan)

	fsMasterNode, err := startFsMasterNode(peerWithLowestRam.MasterContainer)
	if err != nil {
		return err
	}
	nodesConnInfo["fsmaster"] = fsMasterNode
	cc.FSMasterIds = append(cc.FSMasterIds, "fsmaster")

	// go func() {
	etcdNode, err := startEtcdNode(peerWithLowestRam.MasterContainer)
	if err != nil {
		// errChan <- err
		// wg.Done()
		return err
	}
	nodesConnInfo["mretcd"] = etcdNode
	cc.MREtcdIds = append(cc.MREtcdIds, "mretcd")

	// wg.Done()
	// }()

	// go func() {
	fsVolumeNodes, err := startFsVolumeNodes(peers, nodesConnInfo["fsmaster"])
	if err != nil {
		// errChan <- err
		// wg.Done()
		return err
	}
	for i := range fsVolumeNodes {
		volumeName := "fsvolume" + strconv.Itoa(i)
		nodesConnInfo[volumeName] = fsVolumeNodes[i]
		cc.FSVolumeIds = append(cc.FSVolumeIds, volumeName)
	}
	// wg.Done()
	// }()

	// go func() {
	fsFilerNode, err := startFsFilerNode(peerWithLowestRam.MasterContainer, nodesConnInfo["fsmaster"])
	if err != nil {
		// errChan <- err
		// wg.Done()
		return err
	}
	nodesConnInfo["fsfiler"] = fsFilerNode
	cc.FSFilerIds = append(cc.FSFilerIds, "fsfiler")

	// wg.Done()
	// }()

	// wg.Wait()

	// if len(errChan) > 0 {
	// 	return <-errChan
	// }

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
	nodeConn.Httpport.ContainerPort = requiredPort.ContainerPort
	nodeConn.Httpport.HostPort = requiredPort.HostPort

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2petcd")
	if err != nil {
		return nil, err
	}

	err = abs.ExecutePlugin(plugin, con.Id, nil)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("etcd server started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
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

func startMRWorkers(
	connectedPeers []*vms.Peer, numOfOutputFiles int, filePaths ...string) ([]*NodeConnInfo, error) {

	var workerNodes []*NodeConnInfo

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pmrworker")

	if err != nil {
		utils.Logger().Infof(err.Error())
	}

	for i, _ := range connectedPeers {
		peer := connectedPeers[i]
		for j, _ := range peer.OtherContainers {
			con := peer.OtherContainers[j]
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

	fsFilerNode := nodesConnInfo[cc.FSFilerIds[0]]

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

func startMRMaster(con *vms.ContainerInfo, numOfReducers int, cconf string, inputDir string) (*NodeConnInfo, error) {
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

	pluginArgs := []string{nodeConn.Httpport.ContainerPort, inputDir, cconf, strconv.Itoa(numOfReducers)}

	plugin, err := abs.PullPlugin("https://github.com/MFMemon/mrp2pmrmaster")
	if err != nil {
		return nil, err
	}

	err = abs.ExecutePlugin(plugin, con.Id, pluginArgs)
	if err != nil {
		return nil, err
	}

	utils.Logger().Infof("Mapreduce master node started at %v",
		fmt.Sprintf("%v:%v", nodeConn.Ip, nodeConn.Httpport.HostPort),
	)

	return nodeConn, nil
}
