package flow

import (
	"container/list"
	"errors"

	"github.com/dafanshu/mini-flow/sdk"
)

type Workflow struct {
	uflow *Dag
}

type Dag struct {
	udag *sdk.Dag
}

type Node struct {
	unode *sdk.Node
}

// Options options for operation execution
type Options struct {
	// Operation options
	header          map[string]string
	query           map[string][]string
	failureHandler  FuncErrorHandler
	requestHandler  ReqHandler
	responseHandler RespHandler
}

type Option func(*Options)

func Header(key, value string) Option {
	return func(o *Options) {
		o.header[key] = value
	}
}

func Query(key string, value ...string) Option {
	return func(o *Options) {
		array := []string{}
		for _, val := range value {
			array = append(array, val)
		}
		o.query[key] = array
	}
}

func (flow *Workflow) NewDag() *Dag {
	dag := &Dag{}
	dag.udag = sdk.NewDag()
	flow.uflow = dag
	return dag
}

func (flow *Workflow) GetStartNodes() *list.List {
	return flow.uflow.udag.StartV()
}

func (flow *Workflow) GetNodeLeft() int {
	return flow.uflow.udag.NodeIndex()
}

func (flow *Workflow) RemoveExec(nodeIds []string) {
	flow.uflow.udag.Remove(nodeIds)
}

//判断DAG图是否有回路
func (flow *Workflow) IsLegal(nodeIds []string) error {
	legal := flow.uflow.CheckDag()
	if !legal {
		return errors.New("DAG has circle")
	}
	return nil
}

// 根据参数使用依赖关系组装DAG图
func (flow *Workflow) AssembleDag(ins map[string][]string, outs map[string][]string) {
	for out := range outs {
		nodeOut := outs[out]
		if len(nodeOut) == 0 {
			continue
		}
		for _, paramOut := range nodeOut {
			for in := range ins {
				nodeIn := ins[in]
				if len(nodeIn) == 0 {
					continue
				}
				for _, paramIn := range nodeIn {
					if paramOut == paramIn {
						flow.uflow.Edge(out, in)
					}
				}
			}
		}
	}
}

func (dag *Dag) Node(vertex string) *Node {
	node := dag.udag.GetV(vertex)
	if node == nil {
		node = dag.udag.AddV(vertex, []sdk.Operation{})
	}
	return &Node{unode: node}
}

func (dag *Dag) Edge(from, to string) {
	dag.udag.AddE(from, to)
}

func (dag *Dag) CheckDag() bool {
	return sdk.CheckDag(dag.udag)
}

func (o *Options) reset() {
	o.header = map[string]string{}
	o.query = map[string][]string{}
	o.failureHandler = nil
	o.requestHandler = nil
	o.responseHandler = nil
}

func (node *Node) Modify(mod Modifier) *Node {
	newMod := createModifier(mod)
	node.unode.AddOperation(newMod)
	return node
}

func (node *Node) Apply(function string, opts ...Option) *Node {
	newfunc := createFunction(function)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.header) != 0 {
			for key, value := range o.header {
				newfunc.addheader(key, value)
			}
		}
		if len(o.query) != 0 {
			for key, array := range o.query {
				for _, value := range array {
					newfunc.addparam(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newfunc.addFailureHandler(o.failureHandler)
		}
		if o.responseHandler != nil {
			newfunc.addResponseHandler(o.responseHandler)
		}
		if o.requestHandler != nil {
			newfunc.addRequestHandler(o.requestHandler)
		}
	}

	node.unode.AddOperation(newfunc)
	return node
}

func (node *Node) Request(url string, opts ...Option) *Node {
	newHttpRequest := createHttpRequest(url)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.header) != 0 {
			for key, value := range o.header {
				newHttpRequest.addheader(key, value)
			}
		}
		if len(o.query) != 0 {
			for key, array := range o.query {
				for _, value := range array {
					newHttpRequest.addparam(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newHttpRequest.addFailureHandler(o.failureHandler)
		}
		if o.responseHandler != nil {
			newHttpRequest.addResponseHandler(o.responseHandler)
		}
		if o.requestHandler != nil {
			newHttpRequest.addRequestHandler(o.requestHandler)
		}
	}

	node.unode.AddOperation(newHttpRequest)
	return node
}

func (node *Node) In(input ...string) *Node {
	node.unode.AddRebinds(input...)
	return node
}

func (node *Node) Out(output ...string) *Node {
	node.unode.AddProvides(output...)
	return node
}
