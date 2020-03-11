package flow

import "net/http"

// FuncErrorHandler the error handler for OnFailure() options
type FuncErrorHandler func(error) error

// Modifier definition for Modify() call
type Modifier func([]byte) ([]byte, error)

// RespHandler definition for OnResponse() option on operation
type RespHandler func(*http.Response) ([]byte, error)

// Reqhandler definition for RequestHdlr() option on operation
type ReqHandler func(*http.Request)
