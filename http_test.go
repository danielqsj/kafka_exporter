package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestHTTPHandler(t *testing.T) {
	for _, metricsPath := range []string{"/metrics", "/custom-metrics", "/"} {
		t.Run(metricsPath, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metric := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "kafka_exporter_route_test",
				Help: "Test metric for HTTP routing.",
			})
			metric.Set(1)
			registry.MustRegister(metric)
			handler := newHTTPHandler(metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

			metrics := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, metricsPath, nil)
			request.Header.Set("Accept", "text/plain; version=0.0.4")
			handler.ServeHTTP(metrics, request)
			if metrics.Code != http.StatusOK || !strings.Contains(metrics.Body.String(), "kafka_exporter_route_test 1\n") {
				t.Fatalf("metrics response: status %d, body %q", metrics.Code, metrics.Body.String())
			}

			health := httptest.NewRecorder()
			handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if health.Code != http.StatusOK || health.Body.String() != "ok" {
				t.Fatalf("health response: status %d, body %q", health.Code, health.Body.String())
			}

			if metricsPath != "/" {
				landing := httptest.NewRecorder()
				handler.ServeHTTP(landing, httptest.NewRequest(http.MethodGet, "/", nil))
				if landing.Code != http.StatusOK || !strings.Contains(landing.Body.String(), "<a href='"+metricsPath+"'>Metrics</a>") {
					t.Fatalf("landing response: status %d, body %q", landing.Code, landing.Body.String())
				}
			}
		})
	}
}
