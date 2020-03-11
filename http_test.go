package workflow_test

import (
	"fmt"
	"testing"

	"github.com/dafanshu/mini-flow/flow"
	"github.com/dafanshu/simplejson"
	"github.com/stretchr/testify/assert"
)

func TestHttp(t *testing.T) {
	fmt.Println("ssss")
	workflow := new(flow.Workflow)
	dag := workflow.NewDag()

	method := flow.Header("method", "POST")
	param := flow.Query("method", "GET")

	dag.Node("node4").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		param := make(map[string]string)
		param["id"] = "1"
		result.Set("language", "sql")
		result.Set("sql", "SELECT * FROM faas.api_group where id = ?id")
		result.Set("sqlParam", param)
		result.Set("dsId", "1")
		result.Set("outParam", []string{"out_foo_node4"})
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo_node4").Request("http://localhost:8084", method, param)

	dag.Node("node5").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		param := make(map[string]string)
		param["id"] = "1"
		result.Set("language", "sql")
		result.Set("sql", "SELECT * FROM faas.api_group where id = ?id")
		result.Set("sqlParam", param)
		result.Set("dsId", "1")
		result.Set("outParam", []string{"out_foo_node5"})
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo_node5").Request("http://localhost:8084", method, param)

	dag.Node("node6").Modify(func(data []byte) ([]byte, error) {
		fmt.Println("************")
		fmt.Println(string(data))
		result, _ := simplejson.NewJson(data)
		param := make(map[string]string)
		param["ds"] = "kk"
		result.Set("language", "python")
		result.Set("endpoint", "test.py")
		result.Set("bucketName", "mymusic")
		result.Set("stdin", param)
		return result.MarshalJSON()
	}).Request("http://localhost:5002", method, param).In("out_foo_node5", "out_foo_node4").Out("aa")

	dag.Edge("node4", "node6")
	dag.Edge("node5", "node6")

	jsdata, _ := simplejson.NewJson([]byte(`{}`))
	jsdata.Set("in_foo", "in_bar")
	jsdata.Set("request-id", "yurui")
	request, _ := jsdata.MarshalJSON()

	executor := flow.FlowExecutor{Flow: workflow}
	result, err := executor.ExecuteFlow(request)

	assert.Equal(t, err, nil)

	fmt.Println(string(result))
}
