package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	go func() {
		http.ListenAndServe(":2222", promhttp.Handler())
	}()

	s := NewDataService()
	router := s.NewRouter()

	log.Fatal(http.ListenAndServe(":8080", router))
}
