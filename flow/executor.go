package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dafanshu/mini-flow/sdk"
	"github.com/dafanshu/simplejson"
)

type FlowExecutor struct {
	Flow *Workflow
	Ctx  context.Context
	wg   sync.WaitGroup
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

func parseIntOrDurationValue(val string, fallback time.Duration) time.Duration {
	if len(val) > 0 {
		parsedVal, parseErr := strconv.Atoi(val)
		if parseErr == nil && parsedVal >= 0 {
			return time.Duration(parsedVal) * time.Second
		}
	}

	duration, durationErr := time.ParseDuration(val)
	if durationErr != nil {
		return fallback
	}
	return duration
}

func worker(ctx context.Context, wg *sync.WaitGroup, nodeId string, tasks chan *task, taskReturns chan *simplejson.Json, errs chan error) {
	defer wg.Done()
	var sendErr = func(err error) bool {
		if err == nil {
			return false
		}
		fmt.Println(err.Error())
		errs <- err
		return true
	}
	task, ok := <-tasks
	if !ok {
		fmt.Println("Worker: ", nodeId, " : Shutting Down")
		return
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
			result, err = operation.Execute(ctx, task.request, task.options)
		} else {
			result, err = operation.Execute(ctx, result, task.options)
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

	readTimeout := parseIntOrDurationValue(os.Getenv("read_timeout"), 10*time.Second)
	fmt.Println(readTimeout)
	workerCtx, workerCancel := context.WithTimeout(fexec.Ctx, readTimeout)
	defer workerCancel()

	workflow := fexec.Flow
	for workflow.GetNodeLeft() != 0 {
		startNodes := workflow.GetStartNodes()

		nodeSize := startNodes.Len()
		taskCh := make(chan *task, nodeSize)
		taskReturnCh := make(chan *simplejson.Json, nodeSize)
		errCh := make(chan error, nodeSize)

		startNodeIds := make([]string, 0)

		fexec.wg.Add(nodeSize)

		for item := startNodes.Front(); nil != item; item = item.Next() {
			node := item.Value.(*sdk.Node)
			startNodeIds = append(startNodeIds, node.Id)
			go worker(workerCtx, &fexec.wg, node.Id, taskCh, taskReturnCh, errCh)
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
		fexec.wg.Wait()
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
