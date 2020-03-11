package flow

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/s8sg/mini-flow/sdk"
	"github.com/yur/simplejson"
)

var wg sync.WaitGroup

type FlowExecutor struct {
	Flow *Workflow
}

type RawRequest struct {
	Data          []byte
	AuthSignature string
	Query         string
	RequestId     string
}

type task struct {
	request      []byte
	node         *sdk.Node
	options      map[string]interface{}
	parentResult *simplejson.Json
}

type Bolt struct {
	value []byte `json:"value"`
	name  string `json:"name"`
}

func worker(nodeId string, tasks chan *task, taskReturns chan *simplejson.Json, errs chan error) {
	defer wg.Done()

	var sendErr = func(err error) bool {
		if err == nil {
			return false
		}
		fmt.Println(err.Error())
		errs <- err
		return true
	}

	for {
		task, ok := <-tasks
		if !ok {
			fmt.Println("Worker: ", nodeId, " : Shutting Down")
			break
		}
		var result []byte
		var err error
		global, _ := simplejson.NewJson(task.request)
		input, output := task.node.Offer()
		if len(input) > 0 {
			inputMap := simplejson.New()
			for _, v := range input {
				if inputv, ok := global.CheckGet(v); ok {
					inputMap.Set(v, inputv)
				}
				if resultv, ok := task.parentResult.CheckGet(v); ok {
					inputMap.Set(v, resultv)
				}
			}
			result, err = inputMap.MarshalJSON()
			if ok := sendErr(err); ok {
				return
			}
		}
		for _, operation := range task.node.Operations() {
			if result == nil {
				result, err = operation.Execute(task.request, task.options)
			} else {
				result, err = operation.Execute(result, task.options)
			}
			if ok := sendErr(err); ok {
				return
			}
		}
		if result != nil {
			lastResult := simplejson.New()
			outputResp, err1 := simplejson.NewJson(result)
			if ok := sendErr(err1); ok {
				return
			}
			for _, key := range output {
				if value, ok := outputResp.CheckGet(key); ok {
					lastResult.Set(key, value)
				}
			}
			taskReturns <- lastResult
		}
		break
	}
}

func (fexec *FlowExecutor) ExecuteFlow(request []byte) ([]byte, error) {
	globalReq, _ := simplejson.NewJson(request)
	parentResult := simplejson.New()

	options := make(map[string]interface{})
	options["gateway"] = os.Getenv("gateway")

	if reqId, ok := globalReq.CheckGet("request-id"); ok {
		options["request-id"], _ = reqId.String()
	} else {
		options["request-id"] = os.Getenv("request-id")
	}

	workflow := fexec.Flow
	for workflow.GetNodeLeft() != 0 {
		startNodes := workflow.GetStartNodes()

		nodeSize := startNodes.Len()
		taskCh := make(chan *task, nodeSize)
		taskReturnCh := make(chan *simplejson.Json, nodeSize)
		errCh := make(chan error, nodeSize)

		startNodeIds := make([]string, 0)

		wg.Add(nodeSize)

		for item := startNodes.Front(); nil != item; item = item.Next() {
			node := item.Value.(*sdk.Node)
			startNodeIds = append(startNodeIds, node.Id)
			go worker(node.Id, taskCh, taskReturnCh, errCh)
		}

		for item := startNodes.Front(); nil != item; item = item.Next() {
			node := item.Value.(*sdk.Node)
			nodeTask := task{
				node:         node,
				request:      request,
				options:      options,
				parentResult: parentResult,
			}
			taskCh <- &nodeTask
		}
		wg.Wait()
		close(taskCh)
		workflow.RemoveExec(startNodeIds)

		err := handleErr(errCh)
		if err != nil {
			return nil, err
		}
		parentResult = display(taskReturnCh)
	}
	return parentResult.MarshalJSON()
}

func display(results chan *simplejson.Json) *simplejson.Json {
	close(results)
	data := simplejson.New()
	for result := range results {
		for _, key := range result.Keys() {
			data.Set(key, result.Get(key))
		}
	}
	return data
}

func handleErr(errs chan error) error {
	close(errs)
	if len(errs) == 0 {
		return nil
	}
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for err := range errs {
		buffer.WriteString(err.Error())
		buffer.WriteString(",")
	}
	buffer.WriteString("]")
	return errors.New(buffer.String())
}
