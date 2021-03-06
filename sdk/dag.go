package sdk

import (
	"container/list"
	"fmt"
)

var (
	ERR_DUPLICATE_EDGE = fmt.Errorf("dag has duplicate edge")
	ERR_HAS_CIRCLE     = fmt.Errorf("dag has circle")
	INDEGREE           = 1
	OUTDEGREE          = 2
	INCREMENT          = 1
	DECREMENT          = -1
)

type Dag struct {
	Id        string
	nodes     map[string]*Node
	edges     map[string]*list.List
	nodeIndex int
}

type Node struct {
	Id        string
	index     int
	uniqueId  string
	inDegree  int
	outDegree int

	provides   []string
	rebinds    []string
	operations []Operation
}

// 创建图
func NewDag() *Dag {
	dag := new(Dag)
	dag.nodes = make(map[string]*Node)
	dag.Id = "0"
	dag.edges = make(map[string]*list.List)
	return dag
}

// 检查当前图是否符合DAG特性
func CheckDag(dag *Dag) bool {
	cnt := 0
	stack := list.New()
	initInDegree := make(map[string]int, dag.nodeIndex)
	for nodeId := range dag.nodes {
		inDegree := dag.nodes[nodeId].inDegree
		if inDegree == 0 {
			stack.PushBack(nodeId)
		}
		initInDegree[nodeId] = inDegree
	}
	for stack.Len() > 0 {
		item := stack.Back()
		stack.Remove(item)
		cnt++
		adjList := dag.edges[item.Value.(string)]
		for item := adjList.Front(); nil != item; item = item.Next() {
			current := item.Value.(string)
			initInDegree[current]--
			if initInDegree[current] == 0 {
				stack.PushBack(current)
			}
		}
	}
	return cnt == dag.nodeIndex
}

// 新增图的一个顶点V
func (dag *Dag) AddV(id string, options ...[]Operation) *Node {
	node := &Node{Id: id, index: dag.nodeIndex + 1}
	dag.nodeIndex = dag.nodeIndex + 1
	dag.nodes[id] = node
	dag.edges[id] = list.New()
	return node
}

// 新增图的一条边E
func (dag *Dag) AddE(from string, to string) error {
	adjList := dag.edges[from]
	for item := adjList.Front(); nil != item; item = item.Next() {
		if item.Value == to {
			return ERR_DUPLICATE_EDGE
		}
	}
	fromNode := dag.nodes[from]
	if fromNode == nil {
		fromNode = dag.AddV(from)
	}
	fromNode.degree(OUTDEGREE, INCREMENT)
	toNode := dag.nodes[to]
	if toNode == nil {
		toNode = dag.AddV(to)
	}
	toNode.degree(INDEGREE, INCREMENT)
	adjList.PushBack(to)
	return nil
}

// 搜寻图中所有入度为0的顶点
func (dag *Dag) startV() *list.List {
	startNodes := list.New()
	for nodeId := range dag.nodes {
		inDegree := dag.nodes[nodeId].inDegree
		if inDegree == 0 {
			startNodes.PushBack(nodeId)
		}
	}
	return startNodes
}

// 搜寻图中所有入度为0的顶点
func (dag *Dag) StartV() *list.List {
	startNodes := list.New()
	for nodeId := range dag.nodes {
		node := dag.nodes[nodeId]
		inDegree := node.inDegree
		if inDegree == 0 {
			startNodes.PushBack(node)
		}
	}
	return startNodes
}

func (dag *Dag) NodeIndex() int {
	return dag.nodeIndex
}

func (dag *Dag) Remove(nodeIds []string) {
	for i := 0; i < len(nodeIds); i++ {
		nodeId := nodeIds[i]
		delete(dag.nodes, nodeId)
		dag.nodeIndex--
		adjList := dag.edges[nodeId]
		if adjList == nil {
			continue
		}
		for item := adjList.Front(); nil != item; item = item.Next() {
			node := dag.nodes[item.Value.(string)]
			node.degree(INDEGREE, DECREMENT)
		}
		delete(dag.edges, nodeId)
	}
}

// GetNode get a node by Id
func (dag *Dag) GetV(id string) *Node {
	return dag.nodes[id]
}

func (node *Node) degree(inOut int, step int) {
	if inOut == INDEGREE {
		node.inDegree = node.inDegree + step
	}
	if inOut == OUTDEGREE {
		node.outDegree = node.outDegree + step
	}
}

func (node *Node) AddOperation(operation Operation) {
	node.operations = append(node.operations, operation)
}

func (node *Node) Operations() []Operation {
	return node.operations
}

func (node *Node) AddProvides(provide ...string) {
	node.provides = append(node.provides, provide...)
}

func (node *Node) AddRebinds(rebind ...string) {
	node.rebinds = append(node.rebinds, rebind...)
}

func (node *Node) Offer() ([]string, []string) {
	return node.rebinds, node.provides
}
