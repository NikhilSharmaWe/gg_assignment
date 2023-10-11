package main

import (
	"context"
	"fmt"
	"sync"
)

type Service interface {
	HandleDataStore(context.Context, Command) (map[string]string, error)
	HandleDataQueue(context.Context, Command) (map[string]string, error)
}

type DataStore struct {
	Data map[string]string
	sync.RWMutex
}

type DataQueue struct {
	Data    map[string][]string
	Counter map[string]int
	sync.RWMutex
}

type DataService struct {
	DataStore DataStore
	DataQueue DataQueue
}

func NewDataService() *DataService {
	return &DataService{
		DataStore: DataStore{
			Data:    make(map[string]string),
			RWMutex: sync.RWMutex{},
		},

		DataQueue: DataQueue{
			Data:    make(map[string][]string),
			Counter: make(map[string]int),
			RWMutex: sync.RWMutex{},
		},
	}
}

func (s *DataService) HandleDataStore(_ context.Context, c Command) (map[string]string, error) {
	cmd, valid := validateAndGetDataStoreCommand(c.CommandStr)

	if !valid {
		return nil, fmt.Errorf("invalid command")
	}

	switch cmd.Operation {
	case "SET":
		return s.HandleSETCommand(cmd)

	case "GET":
		return s.HandleGETCommand(cmd)

	default:
		return nil, fmt.Errorf("invalid command")
	}
}

func (s *DataService) HandleDataQueue(_ context.Context, c Command) (map[string]string, error) {
	cmd, valid := validateAndGetDataQueueCommand(c.CommandStr)
	if !valid {
		return nil, fmt.Errorf("invalid command")
	}

	switch cmd.Operation {
	case "QPUSH":
		return s.HandleQPUSHCommand(cmd)

	case "QPOP":
		return s.HandleQPOPCommand(cmd)

	case "BQPOP":
		return s.HandleBQPOPCommand(cmd)

	default:
		return nil, fmt.Errorf("invalid command")
	}
}
