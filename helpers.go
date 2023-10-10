package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DataStore struct {
	Data map[string]string
	sync.Mutex
}

type Queue struct {
	Data    map[string][]string
	Counter map[string]int
	Mutex   map[string]*sync.Mutex
}

type ApiFunc func(http.ResponseWriter, *http.Request) *ApiError

type Application struct {
	DataStore DataStore
	Queue     Queue
}

func NewApplication() *Application {
	return &Application{
		DataStore: DataStore{
			Data:  make(map[string]string),
			Mutex: sync.Mutex{},
		},
		Queue: Queue{
			Data:    make(map[string][]string),
			Counter: make(map[string]int),
			Mutex:   make(map[string]*sync.Mutex),
		},
	}
}

func (app *Application) HandleSET(cmd DataStoreCommand) *ApiError {
	var exists bool
	if _, ok := app.DataStore.Data[cmd.Key]; ok {
		exists = true
	}

	if (cmd.Condition == "NX" && exists) || (cmd.Condition == "XX" && !exists) {
		return nil
	}

	if cmd.ExprationTime > 0 {
		go func() {
			time.Sleep(time.Duration(cmd.ExprationTime) * time.Second)
			app.DataStore.Lock()
			defer app.DataStore.Unlock()
			delete(app.DataStore.Data, cmd.Key)
		}()
	}

	app.DataStore.Lock()
	defer app.DataStore.Unlock()
	app.DataStore.Data[cmd.Key] = cmd.Value

	return nil
}

func (app *Application) HandleGET(cmd DataStoreCommand, w http.ResponseWriter) *ApiError {
	if _, ok := app.DataStore.Data[cmd.Key]; !ok {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("key not found")}
	}

	if err := WriteJSON(w, http.StatusOK, map[string]string{"value": app.DataStore.Data[cmd.Key]}); err != nil {
		log.Println(err)
		return &ApiError{http.StatusInternalServerError, fmt.Errorf("internal server error")}
	}

	return nil
}

func (app *Application) HandleQPUSH(cmd QueueCommand, w http.ResponseWriter) *ApiError {
	var exists bool
	if _, ok := app.Queue.Data[cmd.Key]; ok {
		exists = true
	}

	if !exists {
		app.Queue.Data[cmd.Key] = []string{}
		app.Queue.Mutex[cmd.Key] = &sync.Mutex{}
	}

	app.Queue.Mutex[cmd.Key].Lock()
	defer app.Queue.Mutex[cmd.Key].Unlock()
	app.Queue.Data[cmd.Key] = append(app.Queue.Data[cmd.Key], cmd.Values...)

	return nil
}

func (app *Application) HandleQPOP(cmd QueueCommand, w http.ResponseWriter) *ApiError {
	if app.Queue.Counter[cmd.Key] > 0 {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("BQPOP request in progress")}
	}

	if _, ok := app.Queue.Data[cmd.Key]; !ok {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("queue is empty")}
	}

	lastIndex := len(app.Queue.Data[cmd.Key]) - 1
	value := app.Queue.Data[cmd.Key][lastIndex]
	app.Queue.Data[cmd.Key] = app.Queue.Data[cmd.Key][:lastIndex]
	if lastIndex == 0 {
		delete(app.Queue.Data, cmd.Key)
		delete(app.Queue.Counter, cmd.Key)
		delete(app.Queue.Mutex, cmd.Key)
	}

	if err := WriteJSON(w, http.StatusOK, map[string]string{"value": value}); err != nil {
		log.Println(err)
		return &ApiError{http.StatusInternalServerError, fmt.Errorf("internal server error")}
	}

	return nil
}

func (app *Application) HandleBQPOP(cmd QueueCommand, w http.ResponseWriter) *ApiError {
	if app.Queue.Counter[cmd.Key] > 0 {
		return &ApiError{http.StatusBadRequest, fmt.Errorf("BQPOP request in progress")}
	}

	app.Queue.Counter[cmd.Key]++

	defer func() {
		app.Queue.Counter[cmd.Key]--
	}()

	if _, ok := app.Queue.Data[cmd.Key]; !ok {
		time.Sleep(time.Duration(cmd.Timeout) * time.Second)
		if _, ok := app.Queue.Data[cmd.Key]; !ok {
			return &ApiError{http.StatusBadRequest, fmt.Errorf("queue is empty")}
		}
	}

	lastIndex := len(app.Queue.Data[cmd.Key]) - 1
	value := app.Queue.Data[cmd.Key][lastIndex]
	app.Queue.Data[cmd.Key] = app.Queue.Data[cmd.Key][:lastIndex]
	if lastIndex == 0 {
		delete(app.Queue.Data, cmd.Key)
		delete(app.Queue.Counter, cmd.Key)
		delete(app.Queue.Mutex, cmd.Key)
	}

	if err := WriteJSON(w, http.StatusOK, map[string]string{"value": value}); err != nil {
		log.Println(err)
		return &ApiError{http.StatusInternalServerError, fmt.Errorf("internal server error")}
	}

	return nil
}

func MakeHTTPHandleFunc(f ApiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if apiErr := f(w, r); apiErr != nil {
			WriteJSON(w, apiErr.status, map[string]string{"error": apiErr.Error()})
		}
	}
}

func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(v)
}

func validateAndGetDSCommand(cmdStr string) (DataStoreCommand, bool) {
	var cmd DataStoreCommand

	command := strings.Fields(cmdStr)

	if len(command) < 2 || len(command) > 6 {
		return cmd, false
	}

	if command[0] != "SET" && command[0] != "GET" {
		return cmd, false
	}

	if command[0] == "SET" {
		cmd.Operation = "SET"
		cmd.Key = command[1]
		cmd.Value = command[2]

		if len(command) == 4 {
			if command[3] != "XX" && command[3] != "NX" {
				return cmd, false
			}
			cmd.Condition = command[3]
		}

		if len(command) >= 5 {
			if command[3] != "EX" {
				return cmd, false
			}

			t := command[4]
			if sec, err := strconv.Atoi(t); err == nil {
				cmd.ExprationTime = sec
			} else {
				return cmd, false
			}

			if len(command) == 6 {
				if command[5] != "XX" && command[5] != "NX" {
					return cmd, false
				}
				cmd.Condition = command[5]
			}
		}
	}

	if command[0] == "GET" {
		cmd.Operation = "GET"
		if len(command) > 2 {
			return cmd, false
		}
		cmd.Key = command[1]
	}

	return cmd, true
}

func validateAndGetQueueCommand(cmdStr string) (QueueCommand, bool) {
	var cmd QueueCommand

	command := strings.Fields(cmdStr)

	if command[0] != "QPUSH" && command[0] != "QPOP" && command[0] != "BQPOP" {
		return cmd, false
	}

	if command[0] == "QPUSH" && len(command) < 3 {
		return cmd, false
	}

	if command[0] == "BQPOP" && len(command) != 3 {
		return cmd, false
	}

	if command[0] == "QPOP" && len(command) != 2 {
		return cmd, false
	}

	cmd.Operation = command[0]
	cmd.Key = command[1]

	if cmd.Operation == "QPUSH" {
		var values []string
		values = append(values, command[2:]...)
		cmd.Values = values
	}

	if cmd.Operation == "BQPOP" {
		t := command[2]
		if sec, err := strconv.Atoi(t); err == nil {
			cmd.Timeout = sec
		} else {
			return cmd, false
		}
	}

	return cmd, true
}
