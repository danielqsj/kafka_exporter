package main

import (
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var bootstrap_servers = []string{"localhost:9092"}

func TestSmoke(t *testing.T) {
	log.Print("testing " + t.Name())

	if !assumeKafka() {
		t.Skip("Kafka is not running ... skipping the test")
		return
	}

	go runServer()

	execute(func(resp *http.Response) {
		log.Println(resp.Status)

		defer resp.Body.Close()
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Println(string(bytes))
		}
	})
}

func assumeKafka() bool {
	client, err := sarama.NewClient(bootstrap_servers, nil)
	if err != nil {
		return false
	}
	defer client.Close()
	_, err = client.Topics()
	return err == nil
}

func execute(handler func(response *http.Response)) {
	e := errors.New("dummy")
	for e != nil {
		resp, err := http.Get("http://localhost:9304/metrics")
		if err != nil {
			time.Sleep(time.Millisecond * 100)
		}
		e = err
		if resp != nil {
			handler(resp)
		}
	}
}

func runServer() {
	opts := kafkaOpts{}
	opts.uri = bootstrap_servers
	opts.uriZookeeper = []string{"localhost:2181"}
	opts.kafkaVersion = sarama.V1_0_0_0.String()
	opts.metadataRefreshInterval = "30s"
	setup("localhost:9304", "/metrics", ".*", "^$", ".*", "^$", false, opts, nil)
}

func TestFileTokenProvider_ReadsToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("eyJhbGciOiJSUzI1NiJ9.payload.sig"), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	p := &fileTokenProvider{path: path}
	tok, err := p.Token()
	if err != nil {
		t.Fatalf("Token() returned error: %v", err)
	}
	if tok.Token != "eyJhbGciOiJSUzI1NiJ9.payload.sig" {
		t.Errorf("unexpected token: %q", tok.Token)
	}
}

func TestFileTokenProvider_TrimsWhitespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  the-token\n"), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	p := &fileTokenProvider{path: path}
	tok, err := p.Token()
	if err != nil {
		t.Fatalf("Token() returned error: %v", err)
	}
	if tok.Token != "the-token" {
		t.Errorf("expected trimmed token, got %q", tok.Token)
	}
}

func TestFileTokenProvider_MissingFile(t *testing.T) {
	p := &fileTokenProvider{path: filepath.Join(t.TempDir(), "does-not-exist")}
	_, err := p.Token()
	if err == nil {
		t.Fatal("expected error for missing token file, got nil")
	}
	if !strings.Contains(err.Error(), "oauthbearer token file") {
		t.Errorf("error %q does not mention the token file context", err)
	}
}

// SASL enabled without --sasl.mechanism must connect with PLAIN, as the
// README documents, instead of rejecting the empty mechanism.
func TestNewExporter_SASLDefaultMechanism(t *testing.T) {
	for _, mechanism := range []string{"", "plain"} {
		t.Run("mechanism="+mechanism, func(t *testing.T) {
			mock := sarama.NewMockBroker(t, 0)
			t.Cleanup(mock.Close)
			mock.SetHandlerByMap(map[string]sarama.MockResponse{
				"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
					{ApiKey: 3, MaxVersion: 5},  // Metadata
					{ApiKey: 17, MaxVersion: 1}, // SaslHandshake
					{ApiKey: 36, MaxVersion: 0}, // SaslAuthenticate
				}),
				"SaslHandshakeRequest":    sarama.NewMockSaslHandshakeResponse(t).SetEnabledMechanisms([]string{sarama.SASLTypePlaintext}),
				"SaslAuthenticateRequest": sarama.NewMockSaslAuthenticateResponse(t),
				"MetadataRequest":         sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()),
			})

			opts := kafkaOpts{
				uri:                     []string{mock.Addr()},
				useSASL:                 true,
				useSASLHandshake:        true,
				saslUsername:            "user",
				saslPassword:            "pass",
				saslMechanism:           mechanism,
				kafkaVersion:            "2.0.0",
				metadataRefreshInterval: "30s",
				groupMetricsTimeout:     "5m",
			}
			e, err := NewExporter(opts, ".*", "^$", ".*", "^$")
			if err != nil {
				t.Fatalf("NewExporter returned error: %v", err)
			}
			t.Cleanup(func() { _ = e.client.Close() })
		})
	}
}

func unreachableKafkaOpts(lazyConnect bool) kafkaOpts {
	return kafkaOpts{
		uri:                     []string{"127.0.0.1:1"},
		kafkaVersion:            "2.0.0",
		metadataRefreshInterval: "30s",
		groupMetricsTimeout:     "5m",
		lazyConnect:             lazyConnect,
	}
}

func TestNewExporter_FailsWithoutKafkaByDefault(t *testing.T) {
	if _, err := NewExporter(unreachableKafkaOpts(false), ".*", "^$", ".*", "^$"); err == nil {
		t.Fatal("NewExporter succeeded with no reachable broker")
	}
}

func TestNewExporter_LazyConnectStartsWithoutKafka(t *testing.T) {
	e, err := NewExporter(unreachableKafkaOpts(true), ".*", "^$", ".*", "^$")
	if err != nil {
		t.Fatalf("NewExporter returned error: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })

	ch := make(chan prometheus.Metric, 100)
	e.collect(ch)
	close(ch)

	if n := len(ch); n != 0 {
		t.Fatalf("collect sent %d metrics while Kafka is unreachable", n)
	}
	if e.client != nil {
		t.Fatal("client was created although no broker answered")
	}
}

func TestNewExporter_LazyConnectConnectsOnceKafkaAnswers(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: 3, MaxVersion: 5}, // Metadata
		}),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()),
	})
	opts := unreachableKafkaOpts(true)
	opts.uri = []string{mock.Addr()}

	e, err := NewExporter(opts, ".*", "^$", ".*", "^$")
	if err != nil {
		t.Fatalf("NewExporter returned error: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if e.client != nil {
		t.Fatal("client was created before the first scrape")
	}

	if err := e.connect(); err != nil {
		t.Fatalf("connect returned error: %v", err)
	}
	if e.client == nil {
		t.Fatal("connect did not create the client")
	}
}
