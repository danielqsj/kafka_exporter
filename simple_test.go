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
