package mrjob

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/deployments"
	"github.com/MFMemon/mrp2p-orchestrate/mrp2p/utils"
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
		Timeout: time.Second * 5,
	}

	TotalMapTasks        int
	TotalReduceTasks     int
	TotalInputSize       int64
	CompletedMapTasks    int
	CompletedReduceTasks int
)

func Start() error {
	err := mrInit()
	if err != nil {
		return err
	}

	for CompletedReduceTasks < TotalReduceTasks {
		err = mrProgress()
		if err != nil {
			return err
		}
	}
	return nil
}

func mrInit() error {

	responseAwaited := true
	for responseAwaited {

		for i := range deployments.CC.MRMasterIds {
			masterId := deployments.CC.MRMasterIds[i]
			masterInitUrl := fmt.Sprintf(
				"http://%v:%v/init/",
				deployments.NodesConnInfo[masterId].Ip,
				deployments.NodesConnInfo[masterId].Httpport.HostPort,
			)

			res, err := httpClient.Get(masterInitUrl)
			defer res.Body.Close()

			if err != nil {
				return err
			}

			if res.StatusCode == 200 {
				mrInitResponseRaw, err := io.ReadAll(res.Body)
				if err != nil {
					return err
				}

				mrInitResponse := new(MapReduceInitReponse)
				err = json.Unmarshal(mrInitResponseRaw, mrInitResponse)
				if err != nil {
					return err
				}

				TotalMapTasks = mrInitResponse.TotalMapTasks
				TotalReduceTasks = mrInitResponse.TotalReduceTasks
				TotalInputSize = mrInitResponse.JobInputSize

				utils.Logger().Infof("TotalMapTasks: %v", TotalMapTasks)
				utils.Logger().Infof("TotalReduceTasks: %v", TotalReduceTasks)
				utils.Logger().Infof("TotalInputSize: %vMB", TotalInputSize)

				responseAwaited = false
				break
			}

		}
	}

	return nil
}

func mrProgress() error {

	var masterProgressUrl string
	var httpResponse *http.Response
	var httpErr error

	mrProgressResponse := new(MapReduceProgressReponse)

	responseAwaited := true

	for responseAwaited {

		for i := range deployments.CC.MRMasterIds {
			masterId := deployments.CC.MRMasterIds[i]
			masterProgressUrl = fmt.Sprintf(
				"http://%v:%v/progress/",
				deployments.NodesConnInfo[masterId].Ip,
				deployments.NodesConnInfo[masterId].Httpport.HostPort,
			)

			httpResponse, httpErr = httpClient.Get(masterProgressUrl)

			if httpErr != nil {
				return httpErr
			}

			if httpResponse.StatusCode == 200 {
				responseAwaited = false
				break
			}

		}
	}

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

		utils.Logger().Infof("CompletedMapTasks: %v", CompletedMapTasks)
		utils.Logger().Infof("CompletedReduceTasks: %v", CompletedReduceTasks)

		httpResponse, httpErr = httpClient.Get(masterProgressUrl)

		if httpErr != nil {
			return httpErr
		}

		if httpResponse.StatusCode != 200 {
			if httpResponse != nil {
				httpResponse.Body.Close()
			}
			break
		}
	}

	return nil
}
