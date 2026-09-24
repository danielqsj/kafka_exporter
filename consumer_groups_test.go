package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

const (
	describeGroupsAPIKey        int16 = 15
	consumerGroupDescribeAPIKey int16 = 69
)

func TestModernGroupInfoUsesCurrentAssignment(t *testing.T) {
	group := sarama.ConsumerGroupDescription{
		GroupID: "kip-848-group",
		Members: []sarama.ConsumerGroupMemberDescription{
			{
				MemberID: "member-1",
				Assignment: sarama.ConsumerGroupAssignment{
					TopicPartitions: []sarama.ConsumerGroupTopicPartitions{
						{TopicName: "current-topic", Partitions: []int32{0, 1}},
					},
				},
				TargetAssignment: sarama.ConsumerGroupAssignment{
					TopicPartitions: []sarama.ConsumerGroupTopicPartitions{
						{TopicName: "target-topic", Partitions: []int32{2}},
					},
				},
			},
			{
				MemberID: "member-2",
				Assignment: sarama.ConsumerGroupAssignment{
					TopicPartitions: []sarama.ConsumerGroupTopicPartitions{
						{TopicName: "current-topic", Partitions: []int32{1, 2}},
					},
				},
			},
		},
	}

	groupInfo := modernGroupInfoFromDescription(group)

	if groupInfo.id != "kip-848-group" {
		t.Fatalf("unexpected group ID %q", groupInfo.id)
	}
	if groupInfo.memberCount != 2 {
		t.Fatalf("unexpected member count %d", groupInfo.memberCount)
	}

	want := map[string][]int32{"current-topic": {0, 1, 2}}
	if got := groupOffsetPartitions(groupInfo, nil, false); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected current assignments: got %v, want %v", got, want)
	}
	if _, ok := groupInfo.currentAssignments["target-topic"]; ok {
		t.Fatal("target assignment must not be used for offset collection")
	}
}

func TestClassicGroupInfoDecodesAssignments(t *testing.T) {
	group := &sarama.GroupDescription{
		GroupId: "classic-group",
		Members: map[string]*sarama.GroupMemberDescription{
			"member-1": {
				MemberId: "member-1",
				MemberAssignment: encodeClassicAssignment(t, map[string][]int32{
					"topic-a": {2, 0},
				}),
			},
			"member-2": {
				MemberId: "member-2",
			},
		},
	}

	groupInfo, err := classicGroupInfoFromDescription(group)
	if err != nil {
		t.Fatalf("classicGroupInfoFromDescription() returned error: %v", err)
	}

	if groupInfo.memberCount != 2 {
		t.Fatalf("unexpected member count %d", groupInfo.memberCount)
	}

	want := map[string][]int32{"topic-a": {0, 2}}
	if got := groupOffsetPartitions(groupInfo, nil, false); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected assignments: got %v, want %v", got, want)
	}
}

func TestGroupOffsetPartitions(t *testing.T) {
	groupInfo := consumerGroupInfo{
		id:                 "group",
		memberCount:        1,
		currentAssignments: make(map[string]map[int32]struct{}),
	}
	groupInfo.addCurrentAssignment("assigned", []int32{2, 1, 2})

	tests := []struct {
		name      string
		showAll   bool
		offsetMap map[string]map[int32]int64
		want      map[string][]int32
	}{
		{
			name:    "current assignments are deduplicated",
			showAll: false,
			want:    map[string][]int32{"assigned": {1, 2}},
		},
		{
			name:    "all known topic partitions",
			showAll: true,
			offsetMap: map[string]map[int32]int64{
				"topic-a": {2: 100, 0: 50},
				"topic-b": {1: 75},
			},
			want: map[string][]int32{
				"topic-a": {0, 2},
				"topic-b": {1},
			},
		},
		{
			name:    "empty current assignment stays empty",
			showAll: false,
			want:    map[string][]int32{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			group := groupInfo
			if test.name == "empty current assignment stays empty" {
				group = consumerGroupInfo{
					id:                 "empty",
					currentAssignments: make(map[string]map[int32]struct{}),
				}
			}

			if got := groupOffsetPartitions(group, test.offsetMap, test.showAll); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("groupOffsetPartitions() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNewListGroupsRequestUsesGroupTypesWhenAvailable(t *testing.T) {
	tests := []struct {
		version sarama.KafkaVersion
		want    int16
	}{
		{version: sarama.V3_7_0_0, want: 0},
		{version: sarama.V3_8_0_0, want: 5},
		{version: sarama.V4_0_0_0, want: 5},
	}

	for _, test := range tests {
		if got := newListGroupsRequest(test.version).Version; got != test.want {
			t.Fatalf("ListGroups version for Kafka %s = %d, want %d", test.version, got, test.want)
		}
	}
}

func TestDescribeGroupsByTypeRoutesByGroupType(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: describeGroupsAPIKey, MinVersion: 0, MaxVersion: 5},
			{ApiKey: consumerGroupDescribeAPIKey, MinVersion: 0, MaxVersion: 1},
		}),
		"ConsumerGroupDescribeRequest": sarama.NewMockConsumerGroupDescribeResponse(t).
			AddGroupDescription("kip-848-group", sarama.ConsumerGroupDescription{
				GroupState: "Stable",
				Members: []sarama.ConsumerGroupMemberDescription{
					{
						MemberID: "kip-member",
						Assignment: sarama.ConsumerGroupAssignment{
							TopicPartitions: []sarama.ConsumerGroupTopicPartitions{
								{TopicName: "kip-topic", Partitions: []int32{0}},
							},
						},
					},
				},
			}),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t).
			AddGroupDescription("classic-group", &sarama.GroupDescription{
				GroupId: "classic-group",
				State:   "Stable",
				Members: map[string]*sarama.GroupMemberDescription{
					"classic-member": {MemberId: "classic-member"},
				},
			}),
	})

	broker := openGroupTestBroker(t, mock, sarama.V4_0_0_0)
	groupData := map[string]sarama.GroupData{
		"kip-848-group": {GroupType: modernConsumerGroupType},
		"classic-group": {GroupType: "classic"},
	}
	groupInfos, err := describeGroupsByType(broker, []string{"kip-848-group", "classic-group"}, groupData, sarama.V4_0_0_0)
	if err != nil {
		t.Fatalf("describeGroupsByType() returned error: %v", err)
	}

	groups := groupInfosByID(groupInfos)
	if len(groups) != 2 {
		t.Fatalf("unexpected groups: %v", groups)
	}
	if groups["kip-848-group"].memberCount != 1 {
		t.Fatalf("unexpected KIP-848 member count: %d", groups["kip-848-group"].memberCount)
	}
	if groups["classic-group"].memberCount != 1 {
		t.Fatalf("unexpected Classic member count: %d", groups["classic-group"].memberCount)
	}

	for _, history := range mock.History() {
		switch request := history.Request.(type) {
		case *sarama.DescribeGroupsRequest:
			if !reflect.DeepEqual(request.Groups, []string{"classic-group"}) {
				t.Fatalf("DescribeGroups received %v", request.Groups)
			}
		case *sarama.ConsumerGroupDescribeRequest:
			if !reflect.DeepEqual(request.GroupIDs, []string{"kip-848-group"}) {
				t.Fatalf("ConsumerGroupDescribe received %v", request.GroupIDs)
			}
		}
	}
}

func TestDescribeGroupsByTypeKafka37KeepsClassicPath(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: describeGroupsAPIKey, MinVersion: 0, MaxVersion: 5},
		}),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t).
			AddGroupDescription("classic-group", &sarama.GroupDescription{
				GroupId: "classic-group",
				State:   "Stable",
				Members: map[string]*sarama.GroupMemberDescription{
					"member": {MemberId: "member"},
				},
			}),
	})

	broker := openGroupTestBroker(t, mock, sarama.V3_7_0_0)
	groupInfos, err := describeGroupsByType(broker, []string{"classic-group"}, nil, sarama.V3_7_0_0)
	if err != nil {
		t.Fatalf("describeGroupsByType() returned error: %v", err)
	}
	if len(groupInfos) != 1 || groupInfos[0].id != "classic-group" {
		t.Fatalf("unexpected group infos: %v", groupInfos)
	}

	for _, history := range mock.History() {
		if _, ok := history.Request.(*sarama.ConsumerGroupDescribeRequest); ok {
			t.Fatal("ConsumerGroupDescribe must not be used below Kafka 3.8")
		}
	}
}

func TestDescribeGroupsByTypePreservesPerGroupErrors(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: consumerGroupDescribeAPIKey, MinVersion: 0, MaxVersion: 1},
		}),
		"ConsumerGroupDescribeRequest": sarama.NewMockConsumerGroupDescribeResponse(t).
			AddGroupDescription("denied-group", sarama.ConsumerGroupDescription{
				ErrorCode: sarama.ErrGroupAuthorizationFailed,
			}),
	})

	broker := openGroupTestBroker(t, mock, sarama.V4_0_0_0)
	groupData := map[string]sarama.GroupData{
		"denied-group": {GroupType: modernConsumerGroupType},
	}
	groupInfos, err := describeGroupsByType(broker, []string{"denied-group"}, groupData, sarama.V4_0_0_0)
	if !errors.Is(err, sarama.ErrGroupAuthorizationFailed) {
		t.Fatalf("expected group authorization error, got %v", err)
	}
	if len(groupInfos) != 0 {
		t.Fatalf("authorization failure must not produce group infos: %v", groupInfos)
	}
}

func TestDescribeClassicGroupsSkipsDeadGroup(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: describeGroupsAPIKey, MinVersion: 0, MaxVersion: 5},
		}),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t),
	})

	broker := openGroupTestBroker(t, mock, sarama.V4_0_0_0)
	groupInfos, err := describeClassicGroups(broker, []string{"missing-group"})
	if err == nil || !strings.Contains(err.Error(), "is dead") {
		t.Fatalf("expected dead group error, got %v", err)
	}
	if len(groupInfos) != 0 {
		t.Fatalf("dead group must not produce group infos: %v", groupInfos)
	}
}

func openGroupTestBroker(t *testing.T, mock *sarama.MockBroker, version sarama.KafkaVersion) *sarama.Broker {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = version
	config.ApiVersionsRequest = true

	broker := sarama.NewBroker(mock.Addr())
	if err := broker.Open(config); err != nil {
		t.Fatalf("open mock broker: %v", err)
	}
	t.Cleanup(func() {
		if err := broker.Close(); err != nil {
			t.Errorf("close mock broker: %v", err)
		}
	})

	return broker
}

func groupInfosByID(groupInfos []consumerGroupInfo) map[string]consumerGroupInfo {
	groups := make(map[string]consumerGroupInfo, len(groupInfos))
	for _, groupInfo := range groupInfos {
		groups[groupInfo.id] = groupInfo
	}
	return groups
}

func encodeClassicAssignment(t *testing.T, topics map[string][]int32) []byte {
	t.Helper()

	var buf bytes.Buffer
	writeBinary := func(value any) {
		t.Helper()
		if err := binary.Write(&buf, binary.BigEndian, value); err != nil {
			t.Fatalf("encode classic assignment: %v", err)
		}
	}

	writeBinary(int16(0))
	writeBinary(int32(len(topics)))

	topicNames := make([]string, 0, len(topics))
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}
	sort.Strings(topicNames)

	for _, topic := range topicNames {
		writeBinary(int16(len(topic)))
		if _, err := buf.WriteString(topic); err != nil {
			t.Fatalf("encode classic assignment topic: %v", err)
		}

		writeBinary(int32(len(topics[topic])))
		for _, partition := range topics[topic] {
			writeBinary(partition)
		}
	}

	writeBinary(int32(-1))

	return buf.Bytes()
}
