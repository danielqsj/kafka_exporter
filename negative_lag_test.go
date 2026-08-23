package main

import (
	"encoding/binary"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

func encodeAssignment(topic string, partitions ...int32) []byte {
	b := make([]byte, 0, 64)
	b = binary.BigEndian.AppendUint16(b, 0)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(partitions)))
	for _, partition := range partitions {
		b = binary.BigEndian.AppendUint32(b, uint32(partition))
	}
	b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
	return b
}

func TestNegativeLagRefreshOnFilteredTopic(t *testing.T) {
	const (
		includedTopic = "included"
		excludedTopic = "excluded"
		group         = "somegroup"
	)

	assignment := encodeAssignment(excludedTopic, 0)
	probe := sarama.GroupMemberDescription{MemberAssignment: assignment}
	decoded, err := probe.GetMemberAssignment()
	if err != nil {
		t.Fatalf("hand-built assignment does not decode: %v", err)
	}
	if got := decoded.Topics[excludedTopic]; len(got) != 1 || got[0] != 0 {
		t.Fatalf("assignment round-trip mismatch: %#v", decoded.Topics)
	}

	broker := sarama.NewMockBroker(t, 0)
	defer broker.Close()

	oldRegisterer := prometheus.DefaultRegisterer
	oldGatherer := prometheus.DefaultGatherer
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = oldRegisterer
		prometheus.DefaultGatherer = oldGatherer
	})

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(includedTopic, 0, broker.BrokerID()).
			SetLeader(excludedTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(includedTopic, 0, sarama.OffsetNewest, 100).
			SetOffset(includedTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(excludedTopic, 0, sarama.OffsetNewest, 900).
			SetOffset(excludedTopic, 0, sarama.OffsetOldest, 0),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t).
			AddGroup(group, "consumer"),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t).
			AddGroupDescription(group, &sarama.GroupDescription{
				GroupId: group,
				State:   "Stable",
				Members: map[string]*sarama.GroupMemberDescription{
					"member-1": {MemberAssignment: assignment},
				},
			}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset(group, excludedTopic, 0, 500, "", sarama.ErrNoError),
	})

	opts := kafkaOpts{
		uri:                     []string{broker.Addr()},
		uriZookeeper:            []string{"localhost:2181"},
		kafkaVersion:            sarama.V2_0_0_0.String(),
		metadataRefreshInterval: "30s",
		groupMetricsTimeout:     "30s",
		offsetShowAll:           false,
		topicWorkers:            100,
		groupWorkers:            100,
	}

	go setup("localhost:9305", "/metrics", "^"+includedTopic+"$", "^$", ".*", "^$", false, opts, nil)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://localhost:9305/metrics")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return
	}
	t.Fatal("timed out waiting for a successful scrape")
}
