package flow

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

type FaasOperation struct {
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

func (operation *FaasOperation) GetParams() map[string][]string {
	return operation.Param
}

func (operation *FaasOperation) GetHeaders() map[string]string {
	return operation.Header
}

func (operation *FaasOperation) GetId() string {
	id := "modifier"
	switch {
	case operation.Function != "":
		id = operation.Function
	case operation.HttpRequestUrl != "":
		id = "http-req-" + operation.HttpRequestUrl[len(operation.HttpRequestUrl)-16:]
	}
	return id
}

func (operation *FaasOperation) Encode() []byte {
	return []byte("")
}

func (operation *FaasOperation) GetProperties() map[string][]string {
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

func (operation *FaasOperation) Execute(ctx context.Context, data []byte, option map[string]interface{}) ([]byte, error) {
	var result []byte
	var err error

	reqId := fmt.Sprintf("%v", option["request-id"])
	gateway := fmt.Sprintf("%v", option["gateway"])

	switch {
	// If function
	case operation.Function != "":
		fmt.Printf("[Request `%s`] Executing function `%s`\n",
			reqId, operation.Function)
		result, err = executeFunction(ctx, gateway, operation, data)
		if err != nil {
			err = fmt.Errorf("Function(%s), error: function execution failed, %v",
				operation.Function, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}

	// If httpRequest
	case operation.HttpRequestUrl != "":
		fmt.Printf("[Request `%s`] Executing httpRequest `%s`\n",
			reqId, operation.HttpRequestUrl)
		result, err = executeHttpRequest(ctx, operation, data)
		if err != nil {
			err = fmt.Errorf("HttpRequest(%s), error: httpRequest failed, %v",
				operation.HttpRequestUrl, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}
		if result == nil {
			result = []byte("")
		}
	// If modifier
	default:
		fmt.Printf("[Request `%s`] Executing modifier\n", reqId)
		result, err = operation.Mod(data)
		if err != nil {
			err = fmt.Errorf("error: Failed at modifier, %v", err)
			return nil, err
		}
		if result == nil {
			result = []byte("")
		}
		select {
		case <-ctx.Done():
			fmt.Printf("Why? %s\n", ctx.Err())
			return nil, ctx.Err()
		default:

		}
	}

	return result, nil
}

func (operation *FaasOperation) addheader(key string, value string) {
	lKey := strings.ToLower(key)
	operation.Header[lKey] = value
}

func (operation *FaasOperation) addparam(key string, value string) {
	array, ok := operation.Param[key]
	if !ok {
		operation.Param[key] = make([]string, 1)
		operation.Param[key][0] = value
	} else {
		operation.Param[key] = append(array, value)
	}
}

func (operation *FaasOperation) addFailureHandler(handler FuncErrorHandler) {
	operation.FailureHandler = handler
}

func (operation *FaasOperation) addResponseHandler(handler RespHandler) {
	operation.OnResphandler = handler
}

func (operation *FaasOperation) addRequestHandler(handler ReqHandler) {
	operation.Requesthandler = handler
}

// buildURL builds OpenFaaS function execution url for the flowExecuting httpRequest
func buildURL(gateway, rPath, function string) string {
	u, _ := url.Parse(gateway)
	u.Path = path.Join(u.Path, rPath+"/"+function)
	return u.String()
}

// makeQueryStringFromParam create query string from provided query
func makeQueryStringFromParam(params map[string][]string) string {
	if params == nil {
		return ""
	}
	result := ""
	for key, array := range params {
		for _, value := range array {
			keyVal := fmt.Sprintf("%s-%s", key, value)
			if result == "" {
				result = "?" + keyVal
			} else {
				result = result + "&" + keyVal
			}
		}
	}
	return result
}

// buildHttpRequest build upstream request for function
func buildHttpRequest(url string, method string, data []byte, params map[string][]string,
	headers map[string]string) (*http.Request, error) {

	queryString := makeQueryStringFromParam(params)
	if queryString != "" {
		url = url + queryString
	}

	httpReq, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		httpReq.Header.Add(key, value)
	}

	return httpReq, nil
}

// executeFunction executes a function call
func executeFunction(ctx context.Context, gateway string, operation *FaasOperation, data []byte) ([]byte, error) {
	var err error
	var result []byte

	name := operation.Function
	params := operation.GetParams()
	headers := operation.GetHeaders()

	funcUrl := buildURL("http://"+gateway, "function", name)

	method := os.Getenv("default-method")
	if method == "" {
		method = "POST"
	}

	if m, ok := headers["method"]; ok {
		method = m
	}

	headers["Content-Type"] = "application/json"

	httpReq, err := buildHttpRequest(funcUrl, method, data, params, headers)
	if err != nil {
		return []byte{}, fmt.Errorf("cannot connect to Function on URL: %s", funcUrl)
	}

	if operation.Requesthandler != nil {
		operation.Requesthandler(httpReq)
	}

	client := &http.Client{}
	resp, err := client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return []byte{}, err
	}

	defer resp.Body.Close()
	if operation.OnResphandler != nil {
		result, err = operation.OnResphandler(resp)
	} else {
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			err = fmt.Errorf("invalid return status %d while connecting %s", resp.StatusCode, funcUrl)
			result, _ = ioutil.ReadAll(resp.Body)
		} else {
			result, err = ioutil.ReadAll(resp.Body)
		}
	}

	return result, err
}

// executeHttpRequest executes a httpRequest
func executeHttpRequest(ctx context.Context, operation *FaasOperation, data []byte) ([]byte, error) {
	var err error
	var result []byte

	httpUrl := operation.HttpRequestUrl
	params := operation.GetParams()
	headers := operation.GetHeaders()

	method := os.Getenv("default-method")
	if method == "" {
		method = "POST"
	}

	if m, ok := headers["method"]; ok {
		method = m
	}

	headers["Content-Type"] = "application/json"

	httpReq, err := buildHttpRequest(httpUrl, method, data, params, headers)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Function on URL: %s", httpUrl)
	}

	if operation.Requesthandler != nil {
		operation.Requesthandler(httpReq)
	}

	client := &http.Client{}
	resp, err := client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if operation.OnResphandler != nil {
		_, err = operation.OnResphandler(resp)
	} else {
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			err = fmt.Errorf("invalid return status %d while connecting %s", resp.StatusCode, httpUrl)
			result, _ = ioutil.ReadAll(resp.Body)
		} else {
			result, err = ioutil.ReadAll(resp.Body)
		}
	}
	return result, err
}

// createFunction Create a function with execution name
func createFunction(name string) *FaasOperation {
	operation := &FaasOperation{}
	operation.Function = name
	operation.Header = make(map[string]string)
	operation.Param = make(map[string][]string)
	return operation
}

// createModifier Create a modifier
func createModifier(mod Modifier) *FaasOperation {
	operation := &FaasOperation{}
	operation.Mod = mod
	return operation
}

// createHttpRequest Create a httpRequest
func createHttpRequest(url string) *FaasOperation {
	operation := &FaasOperation{}
	operation.HttpRequestUrl = url
	operation.Header = make(map[string]string)
	operation.Param = make(map[string][]string)
	return operation
}
