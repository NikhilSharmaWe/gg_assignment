package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

type Command struct {
	CommandStr string `json:"command"`
}

type DataStoreCommand struct {
	Operation     string
	Key           string
	Value         string
	ExprationTime int
	Condition     string
}

type QueueCommand struct {
	Operation string
	Key       string
	Values    []string
	Timeout   int
}

type ApiError struct {
	status int
	error
}

func (app *Application) NewRouter() *mux.Router {
	mux := mux.NewRouter()
	mux.HandleFunc("/datastore", MakeHTTPHandleFunc(app.HandleDataStore)).Methods(http.MethodPost)
	mux.HandleFunc("/queue", MakeHTTPHandleFunc(app.HandleQueue)).Methods(http.MethodPost)

	return mux
}

func (app *Application) HandleDataStore(w http.ResponseWriter, r *http.Request) *ApiError {
	var c Command

	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		return &ApiError{http.StatusBadRequest, err}
	}

	cmd, valid := validateAndGetDSCommand(c.CommandStr)
	if !valid {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("invalid command")}
	}

	switch cmd.Operation {
	case "SET":
		return app.HandleSET(cmd)

	case "GET":
		return app.HandleGET(cmd, w)

	default:
		return &ApiError{http.StatusBadRequest, fmt.Errorf("invalid command")}
	}
}

func (app *Application) HandleQueue(w http.ResponseWriter, r *http.Request) *ApiError {
	var c Command

	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		return &ApiError{http.StatusBadRequest, err}
	}

	cmd, valid := validateAndGetQueueCommand(c.CommandStr)
	if !valid {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("invalid command")}
	}

	switch cmd.Operation {
	case "QPUSH":
		return app.HandleQPUSH(cmd, w)

	case "QPOP":
		return app.HandleQPOP(cmd, w)

	case "BQPOP":
		return app.HandleBQPOP(cmd, w)

	default:
		return &ApiError{http.StatusBadRequest, fmt.Errorf("invalid command")}
	}
}
