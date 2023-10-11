package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PromMetrics struct {
	reqCounter prometheus.Counter
	reqLatency prometheus.Histogram
}

func NewPromMetrics() *PromMetrics {
	reqCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "request_counter",
		Help: "counter of the requests the server receives",
	})

	reqLatency := promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "request_latency",
		Help:    "latency metrics of the requests the server receives",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})

	return &PromMetrics{
		reqCounter: reqCounter,
		reqLatency: reqLatency,
	}
}

func (p *PromMetrics) PromMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		p.reqCounter.Inc()
		next.ServeHTTP(w, r)
		latency := time.Since(start).Seconds()
		p.reqLatency.Observe(latency)
	})
}
