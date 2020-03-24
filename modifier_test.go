package workflow_test

import (
	"context"
	"fmt"
	"github.com/dafanshu/mini-flow/flow"
	"github.com/dafanshu/simplejson"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestModiferInitData(t *testing.T) {
	workflow := new(flow.Workflow)
	dag := workflow.NewDag()

	dag.Node("node7").Modify(func(data []byte) ([]byte, error) {
		fmt.Println(">>>>>>>")
		fmt.Println(len(data))
		fmt.Println(string(data) == "")
		fmt.Println(">>>>>>>")
		return data, nil
	})

	request, _ := simplejson.NewJson([]byte("{}"))
	request.Set("request-id", "bar")
	data, _ := request.Bytes()
	executor := flow.FlowExecutor{Flow: workflow}
	result, err := executor.ExecuteFlow(data)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	fmt.Println(result)
	//raw := `{"foo":"bar"}`
	assert.Equal(t, nil, err)
}

func TestModifer(t *testing.T) {
	fmt.Println("ssss")
	workflow := new(flow.Workflow)
	dag := workflow.NewDag()

	dag.Node("node5").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		result.Set("out_foo_node5", "out_bar000_node5")
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo_node5")

	dag.Node("node6").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		result.Set("out_foo", "out_bar000")
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo")

	dag.Node("node7").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		fmt.Println(result)
		result.Set("test", "test")
		out_foo_node5, _ := result.Get("out_foo_node5").String()
		out_foo, _ := result.Get("out_foo").String()
		result.Set("node_foo", fmt.Sprintf("%s-%s", out_foo, out_foo_node5))
		res, _ := result.MarshalJSON()
		return res, nil
	}).In("out_foo", "out_foo_node5").Out("node_foo")

	dag.Edge("node5", "node7")
	dag.Edge("node6", "node7")

	jsdata, _ := simplejson.NewJson([]byte(`{}`))
	jsdata.Set("in_foo", "in_bar")
	jsdata.Set("request-id", "yurui")
	request, _ := jsdata.MarshalJSON()

	executor := flow.FlowExecutor{Flow: workflow}
	result, err := executor.ExecuteFlow(request)

	assert.Equal(t, err, nil)

	target := `{"node_foo":"out_bar000-out_bar000_node5"}`
	assert.Equal(t, target, string(result))

	fmt.Println(">>>>>>>>>>")
	fmt.Println(string(result))
}

func TestReturnJson(t *testing.T) {
	fmt.Println(">>>>TestReturnJson")
	workflow := new(flow.Workflow)
	dag := workflow.NewDag()

	dag.Node("node5").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		js, _ := simplejson.NewJson([]byte(`{"this":{"a":0.1,"b":"bb","c":"cc"}}`))
		result.Set("out_foo_node5", js)
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo_node5")

	dag.Node("node6").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		result.Set("out_foo", "out_bar000")
		return result.MarshalJSON()
	}).In("in_foo").Out("out_foo")

	dag.Node("node7").Modify(func(data []byte) ([]byte, error) {
		result, _ := simplejson.NewJson(data)
		sub := simplejson.New()
		out_foo_node5, _ := result.CheckGet("out_foo_node5")
		out_foo, _ := result.CheckGet("out_foo")

		sub.Set("out_foo_node5", out_foo_node5)
		sub.Set("out_foo", out_foo)

		result.Set("node_foo", sub)
		res, _ := result.MarshalJSON()
		return res, nil
	}).In("out_foo", "out_foo_node5").Out("node_foo")

	dag.Edge("node5", "node7")
	dag.Edge("node6", "node7")

	jsdata, _ := simplejson.NewJson([]byte(`{}`))
	jsdata.Set("in_foo", "in_bar")
	jsdata.Set("request-id", "yurui")
	request, _ := jsdata.MarshalJSON()

	executor := flow.FlowExecutor{Flow: workflow, Ctx: context.TODO()}
	result, err := executor.ExecuteFlow(request)

	assert.Equal(t, nil, err)

	target := `{"node_foo":{"out_foo":"out_bar000","out_foo_node5":{"this":{"a":0.1,"b":"bb","c":"cc"}}}}`
	assert.Equal(t, target, string(result))

	fmt.Println(">>>>>>>>>>")
	fmt.Println(string(result))
}
