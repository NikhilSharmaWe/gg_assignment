package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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

type DataQueueCommand struct {
	Operation string
	Key       string
	Values    []string
	Timeout   int
}

func (s *DataService) HandleSETCommand(cmd DataStoreCommand) (map[string]string, error) {
	var exists bool
	if _, ok := s.DataStore.Data[cmd.Key]; ok {
		exists = true
	}

	if (cmd.Condition == "NX" && exists) || (cmd.Condition == "XX" && !exists) {
		return nil, fmt.Errorf("data not updated")
	}

	if cmd.ExprationTime > 0 {
		go func() {
			time.Sleep(time.Duration(cmd.ExprationTime) * time.Second)
			s.DataStore.Lock()
			defer s.DataStore.Unlock()
			delete(s.DataStore.Data, cmd.Key)
		}()
	}

	s.DataStore.Lock()
	defer s.DataStore.Unlock()
	s.DataStore.Data[cmd.Key] = cmd.Value

	return nil, nil
}

func (s *DataService) HandleGETCommand(cmd DataStoreCommand) (map[string]string, error) {
	s.DataStore.RLock()
	defer s.DataStore.RUnlock()
	if _, ok := s.DataStore.Data[cmd.Key]; !ok {
		return nil, fmt.Errorf("key not found")
	}

	response := map[string]string{"value": s.DataStore.Data[cmd.Key]}

	return response, nil
}

func (s *DataService) HandleQPUSHCommand(cmd DataQueueCommand) (map[string]string, error) {
	var exists bool

	if _, ok := s.DataQueue.Data[cmd.Key]; ok {
		exists = true
	}

	if !exists {
		s.DataQueue.Data[cmd.Key] = []string{}
	}

	s.DataQueue.Lock()
	defer s.DataQueue.Unlock()

	s.DataQueue.Data[cmd.Key] = append(s.DataQueue.Data[cmd.Key], cmd.Values...)

	return nil, nil
}

func (s *DataService) HandleQPOPCommand(cmd DataQueueCommand) (map[string]string, error) {
	if s.DataQueue.Counter[cmd.Key] > 0 {
		return nil, fmt.Errorf("BQPOP request in progress")
	}

	if _, ok := s.DataQueue.Data[cmd.Key]; !ok {
		return nil, fmt.Errorf("DataQueue is empty")
	}

	s.DataQueue.Lock()
	defer s.DataQueue.Unlock()

	lastIndex := len(s.DataQueue.Data[cmd.Key]) - 1
	value := s.DataQueue.Data[cmd.Key][lastIndex]
	s.DataQueue.Data[cmd.Key] = s.DataQueue.Data[cmd.Key][:lastIndex]
	if lastIndex == 0 {
		delete(s.DataQueue.Data, cmd.Key)
		delete(s.DataQueue.Counter, cmd.Key)
	}

	response := map[string]string{"value": value}

	return response, nil
}

func (s *DataService) HandleBQPOPCommand(cmd DataQueueCommand) (map[string]string, error) {
	if s.DataQueue.Counter[cmd.Key] > 0 {
		return nil, fmt.Errorf("BQPOP request in progress")
	}

	s.DataQueue.Counter[cmd.Key]++

	if _, ok := s.DataQueue.Data[cmd.Key]; !ok {
		time.Sleep(time.Duration(cmd.Timeout) * time.Second)
		if _, ok := s.DataQueue.Data[cmd.Key]; !ok {
			s.DataQueue.Counter[cmd.Key]--
			return nil, fmt.Errorf("DataQueue is empty")
		}
	}

	s.DataQueue.Lock()
	defer s.DataQueue.Unlock()

	lastIndex := len(s.DataQueue.Data[cmd.Key]) - 1
	value := s.DataQueue.Data[cmd.Key][lastIndex]
	s.DataQueue.Data[cmd.Key] = s.DataQueue.Data[cmd.Key][:lastIndex]

	s.DataQueue.Counter[cmd.Key]--

	if lastIndex == 0 {
		delete(s.DataQueue.Data, cmd.Key)
		delete(s.DataQueue.Counter, cmd.Key)
	}

	response := map[string]string{"value": value}

	return response, nil
}

func validateAndGetDataStoreCommand(cmdStr string) (DataStoreCommand, bool) {
	var cmd DataStoreCommand

	command := strings.Fields(cmdStr)

	if len(command) < 2 || len(command) > 6 {
		return cmd, false
	}

	if command[0] != "SET" && command[0] != "GET" {
		return cmd, false
	}

	if command[0] == "SET" {
		if len(command) <= 2 {
			return cmd, false
		}

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
		if len(command) > 2 {
			return cmd, false
		}
		cmd.Operation = "GET"
		cmd.Key = command[1]
	}

	return cmd, true
}

func validateAndGetDataQueueCommand(cmdStr string) (DataQueueCommand, bool) {
	var cmd DataQueueCommand

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
