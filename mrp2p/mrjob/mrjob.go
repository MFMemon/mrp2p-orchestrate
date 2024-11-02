package mrjob

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/deployments"
	"github.com/schollz/progressbar/v2"
)

type MapReduceInitReponse struct {
	TotalMapTasks    int   `json:"m"`
	TotalReduceTasks int   `json:"r"`
	JobInputSize     int64 `json:"inputSize"`
}

type MapReduceProgressReponse struct {
	CompletedMapTasks    int `json:"mapTasksCompleted"`
	CompletedReduceTasks int `json:"reduceTasksCompleted"`
}

var (
	httpClient = http.Client{
		Timeout: time.Second * 20,
	}

	TotalMapTasks        int
	TotalReduceTasks     int
	TotalInputSize       int64
	CompletedMapTasks    int
	CompletedReduceTasks int
	conf                 *deployments.ClusterConfig
)

func Start() error {

	f, err := os.ReadFile(deployments.CCPath)
	if err != nil {
		return err
	}

	conf = new(deployments.ClusterConfig)
	err = json.Unmarshal(f, conf)
	if err != nil {
		return err
	}

	err = mrInit()
	if err != nil {
		return err
	}

	for CompletedReduceTasks < TotalReduceTasks {
		err = mrProgress()
		if err != nil {
			return err
		}
	}
	fmt.Println()
	return nil
}

func mrInit() error {

	responseAwaited := true
	for responseAwaited {

		for i := range conf.MRMasterIds {
			masterId := conf.MRMasterIds[i]
			masterInitUrl := fmt.Sprintf(
				"http://%v:%v/init/",
				conf.MRP2PClusterInfo[masterId].Ip,
				conf.MRP2PClusterInfo[masterId].Httpport.HostPort,
			)

			fmt.Printf("sending init request to %v\n", masterInitUrl)

			res, _ := httpClient.Get(masterInitUrl)

			// if err != nil {
			// 	return err
			// }

			if res == nil {
				continue
			}

			if res.StatusCode == 200 {
				mrInitResponseRaw, _ := io.ReadAll(res.Body)
				// if err != nil {
				// 	return err
				// }

				mrInitResponse := new(MapReduceInitReponse)
				json.Unmarshal(mrInitResponseRaw, mrInitResponse)
				// if err != nil {
				// 	return err
				// }

				TotalMapTasks = mrInitResponse.TotalMapTasks
				TotalReduceTasks = mrInitResponse.TotalReduceTasks
				TotalInputSize = mrInitResponse.JobInputSize

				if TotalMapTasks <= 0 {
					continue
				}

				responseAwaited = false

				defer res.Body.Close()
				break
			}
			time.Sleep(time.Second * 3)
		}
	}

	fmt.Printf("\nTOTAL MAP TASKS: %v\n", TotalMapTasks)
	fmt.Printf("TOTAL REDUCE TASKS: %v\n", TotalReduceTasks)
	fmt.Printf("TOTAL INPUT SIZE: %v\n\n", TotalInputSize)

	return nil
}

func mrProgress() error {

	var masterProgressUrl string
	var httpResponse *http.Response
	// var httpErr error

	pBar := progressbar.New(TotalMapTasks + TotalReduceTasks)

	// for i := 0; i < 100; i++ {
	// 	mapBar.Add(1)
	// 	time.Sleep(40 * time.Millisecond)
	// }

	mrProgressResponse := new(MapReduceProgressReponse)

	responseAwaited := true

	for responseAwaited {

		for i := range conf.MRMasterIds {
			masterId := conf.MRMasterIds[i]
			masterProgressUrl = fmt.Sprintf(
				"http://%v:%v/progress/",
				conf.MRP2PClusterInfo[masterId].Ip,
				conf.MRP2PClusterInfo[masterId].Httpport.HostPort,
			)

			httpResponse, _ = httpClient.Get(masterProgressUrl)

			// if httpErr != nil {
			// 	return httpErr
			// }

			if httpResponse == nil {
				continue
			}

			if httpResponse.StatusCode == 200 {
				responseAwaited = false
				break
			}

		}
	}

	prevProgress := CompletedMapTasks + CompletedReduceTasks

	for CompletedReduceTasks < TotalReduceTasks {
		mrProgressResponseRaw, err := io.ReadAll(httpResponse.Body)
		if err != nil {
			return err
		}

		err = json.Unmarshal(mrProgressResponseRaw, mrProgressResponse)
		if err != nil {
			return err
		}

		httpResponse.Body.Close()

		CompletedMapTasks = max(CompletedMapTasks, mrProgressResponse.CompletedMapTasks)
		CompletedReduceTasks = max(CompletedReduceTasks, mrProgressResponse.CompletedReduceTasks)

		pBar.Add(CompletedMapTasks + CompletedReduceTasks - prevProgress)

		// utils.Logger().Infof("CompletedMapTasks: %v", CompletedMapTasks)
		// utils.Logger().Infof("CompletedReduceTasks: %v", CompletedReduceTasks)

		httpResponse, _ = httpClient.Get(masterProgressUrl)

		// if httpErr != nil {
		// 	return httpErr
		// }

		if httpResponse == nil {
			break
		}

		if httpResponse != nil {
			if httpResponse.StatusCode != 200 {
				httpResponse.Body.Close()
				break
			}
		}
		prevProgress = CompletedMapTasks + CompletedReduceTasks
	}

	return nil
}
