package workflow

import (
	"encoding/json"
	"fmt"
	"github.com/s8sg/mini-flow/flow"
)

func modify(data []byte, add string) []byte {
	resp := make(map[string][]byte)
	json.Unmarshal(data, &resp)

	for k, v := range resp {
		dataadd := string(v) + add
		fmt.Println(dataadd)
		resp[k] = []byte(dataadd)
	}

	result, _ := json.Marshal(resp)
	return result
}

func main() {
	workflow := new(flow.Workflow)
	dag := workflow.NewDag()

	method := flow.Header("method", "POST")
	param := flow.Query("method", "GET")

	//{
	//	"language":"sql",
	//	"sql":"select uuid()",
	//	"param":{},
	//	"dsId": 1
	//}

	dag.Node("node6").Modify(func(data []byte) ([]byte, error) {
		// do something
		//fmt.Println("----************>>>")
		fmt.Println("node6node6node6", string(data))
		//fmt.Println("----************<<<")
		param := make(map[string]string)
		//jsonByte, _ := json.Marshal(param)
		if len(data) > 0 {
			err := json.Unmarshal(data, &param)
			if err != nil {
				panic(err)
			}
		}

		dataMap := make(map[string]interface{})
		dataMap["language"] = "sql"
		dataMap["sql"] = "SELECT * FROM log_his limit 0,1"
		dataMap["param"] = param
		dataMap["dsId"] = "1"
		request, _ := json.Marshal(dataMap)
		fmt.Println("------------")
		fmt.Println(string(request))
		fmt.Println("____________")
		return request, nil
	}).In("foo", "fuck").Out("stdout").Modify(func(data []byte) ([]byte, error) {
		dataMap := make(map[string]interface{})
		//jsonByte, _ := json.Marshal(param)
		fmt.Println(string(data))
		if len(data) > 0 {
			err := json.Unmarshal(data, &dataMap)
			if err != nil {
				panic(err)
			}
		}
		dataMap["yur"] = "yur"
		request, _ := json.Marshal(dataMap)
		fmt.Println("----9999--------")
		fmt.Println(string(request))
		fmt.Println("____999________")
		return request, nil
	}).Request("http://localhost:8084", method, param)

	dataMap := make(map[string]string)
	dataMap["kk"] = "aaaaaaa"
	dataMap["foo"] = "foooo"
	dataMap["fuck"] = "fuck"
	request, _ := json.Marshal(dataMap)
	fmt.Println(string(request))
	executor := flow.FlowExecutor{Flow: workflow}
	result, err := executor.ExecuteFlow(request)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	fmt.Println(string(result))
	var mapResult map[string]interface{}
	json.Unmarshal(result, &mapResult)
	fmt.Println(mapResult["stdout"])
}
