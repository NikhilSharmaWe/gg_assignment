package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-kit/kit/endpoint"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
)

func (s *DataService) NewRouter() *mux.Router {
	mux := mux.NewRouter()

	dataStoreHandler := httptransport.NewServer(
		makeDataStoreEndpoint(s),
		decodeRequest,
		encodeResponse,
	)

	queueHandler := httptransport.NewServer(
		makeDataQueueEndpoint(s),
		decodeRequest,
		encodeResponse,
	)

	metrics := NewPromMetrics()
	mux.Use(metrics.PromMiddleware)

	mux.Handle("/datastore", dataStoreHandler).Methods(http.MethodPost)
	mux.Handle("/queue", queueHandler).Methods(http.MethodPost)

	return mux
}

func makeDataStoreEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		cmd := request.(Command)
		response, err := s.HandleDataStore(ctx, cmd)
		return response, err
	}
}

func makeDataQueueEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		c := request.(Command)
		response, err := s.HandleDataQueue(ctx, c)
		return response, err
	}
}

func decodeRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var c Command
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		return nil, err
	}
	return c, nil
}

func encodeResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	return json.NewEncoder(w).Encode(response)
}
