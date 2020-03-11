package flow

import (
	"strings"
)

type GrpcOperation struct {
	// FaasOperations
	Function       string   // The name of the function
	HttpRequestUrl string   // HttpRequest Url
	Mod            Modifier // Modifier

	// Optional Options
	Header map[string]string   // The HTTP call header
	Param  map[string][]string // The Parameter in Query string

	FailureHandler FuncErrorHandler // The Failure handler of the operation
	Requesthandler ReqHandler       // The http request handler of the operation
	OnResphandler  RespHandler      // The http Resp handler of the operation
}

func (operation *GrpcOperation) GetParams() map[string][]string {
	return operation.Param
}

func (operation *GrpcOperation) GetHeaders() map[string]string {
	return operation.Header
}

func (operation *GrpcOperation) GetId() string {
	id := "modifier"
	switch {
	case operation.Function != "":
		id = operation.Function
	case operation.HttpRequestUrl != "":
		id = "http-req-" + operation.HttpRequestUrl[len(operation.HttpRequestUrl)-16:]
	}
	return id
}

func (operation *GrpcOperation) Encode() []byte {
	return []byte("")
}

func (operation *GrpcOperation) GetProperties() map[string][]string {
	result := make(map[string][]string)

	isMod := "false"
	isFunction := "false"
	isHttpRequest := "false"
	hasFailureHandler := "false"
	hasResponseHandler := "false"

	if operation.Mod != nil {
		isMod = "true"
	}
	if operation.Function != "" {
		isFunction = "true"
	}
	if operation.HttpRequestUrl != "" {
		isHttpRequest = "true"
	}

	result["isMod"] = []string{isMod}
	result["isFunction"] = []string{isFunction}
	result["isHttpRequest"] = []string{isHttpRequest}
	result["hasFailureHandler"] = []string{hasFailureHandler}
	result["hasResponseHandler"] = []string{hasResponseHandler}

	return result
}

func (operation *GrpcOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {

	return nil, nil
}

func (operation *GrpcOperation) addheader(key string, value string) {
	lKey := strings.ToLower(key)
	operation.Header[lKey] = value
}

func (operation *GrpcOperation) addparam(key string, value string) {
	array, ok := operation.Param[key]
	if !ok {
		operation.Param[key] = make([]string, 1)
		operation.Param[key][0] = value
	} else {
		operation.Param[key] = append(array, value)
	}
}

func (operation *GrpcOperation) addFailureHandler(handler FuncErrorHandler) {
	operation.FailureHandler = handler
}

func (operation *GrpcOperation) addResponseHandler(handler RespHandler) {
	operation.OnResphandler = handler
}

func (operation *GrpcOperation) addRequestHandler(handler ReqHandler) {
	operation.Requesthandler = handler
}
