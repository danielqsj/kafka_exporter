package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/IBM/sarama"
	"k8s.io/klog/v2"
)

const modernConsumerGroupType = "consumer"

type consumerGroupInfo struct {
	id                 string
	memberCount        int
	currentAssignments map[string]map[int32]struct{}
}

func (g consumerGroupInfo) addCurrentAssignment(topic string, partitions []int32) {
	if topic == "" {
		return
	}

	if _, ok := g.currentAssignments[topic]; !ok {
		g.currentAssignments[topic] = make(map[int32]struct{})
	}

	for _, partition := range partitions {
		g.currentAssignments[topic][partition] = struct{}{}
	}
}

func newListGroupsRequest(version sarama.KafkaVersion) *sarama.ListGroupsRequest {
	request := &sarama.ListGroupsRequest{}
	if version.IsAtLeast(sarama.V3_8_0_0) {
		request.Version = 5
	}

	return request
}

func describeGroupsByType(broker *sarama.Broker, groupIDs []string, groupData map[string]sarama.GroupData, version sarama.KafkaVersion) ([]consumerGroupInfo, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}

	if !version.IsAtLeast(sarama.V3_8_0_0) {
		return describeClassicGroups(broker, groupIDs)
	}

	var classicGroupIDs, kip848GroupIDs []string
	for _, groupID := range groupIDs {
		if groupData[groupID].GroupType == modernConsumerGroupType {
			kip848GroupIDs = append(kip848GroupIDs, groupID)
		} else {
			classicGroupIDs = append(classicGroupIDs, groupID)
		}
	}

	classicGroupInfos, classicErr := describeClassicGroups(broker, classicGroupIDs)
	kip848GroupInfos, kip848Err := describeKIP848Groups(broker, kip848GroupIDs, version)

	return append(classicGroupInfos, kip848GroupInfos...), errors.Join(classicErr, kip848Err)
}

func describeKIP848Groups(broker *sarama.Broker, groupIDs []string, version sarama.KafkaVersion) ([]consumerGroupInfo, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}

	request := sarama.NewConsumerGroupDescribeRequest(version)
	request.GroupIDs = groupIDs

	response, err := broker.ConsumerGroupDescribe(request)
	if err != nil {
		return nil, fmt.Errorf("consumer group describe request: %w", err)
	}

	groupInfos := make([]consumerGroupInfo, 0, len(response.Groups))
	var describeErrors []error

	for _, group := range response.Groups {
		if group.ErrorCode != sarama.ErrNoError {
			describeErrors = append(describeErrors, consumerGroupDescribeError(group))
			continue
		}

		if group.GroupState == "Dead" {
			describeErrors = append(describeErrors, fmt.Errorf("consumer group %q is dead", group.GroupID))
			continue
		}

		groupInfos = append(groupInfos, modernGroupInfoFromDescription(group))
	}

	return groupInfos, errors.Join(describeErrors...)
}

func consumerGroupDescribeError(group sarama.ConsumerGroupDescription) error {
	if group.ErrorMessage != nil && *group.ErrorMessage != "" {
		return fmt.Errorf("cannot describe consumer group %q: %s: %w", group.GroupID, *group.ErrorMessage, group.ErrorCode)
	}

	return fmt.Errorf("cannot describe consumer group %q: %w", group.GroupID, group.ErrorCode)
}

func modernGroupInfoFromDescription(group sarama.ConsumerGroupDescription) consumerGroupInfo {
	groupInfo := consumerGroupInfo{
		id:                 group.GroupID,
		memberCount:        len(group.Members),
		currentAssignments: make(map[string]map[int32]struct{}),
	}

	for _, member := range group.Members {
		for _, topic := range member.Assignment.TopicPartitions {
			groupInfo.addCurrentAssignment(topic.TopicName, topic.Partitions)
		}
	}

	return groupInfo
}

func describeClassicGroups(broker *sarama.Broker, groupIDs []string) ([]consumerGroupInfo, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}

	response, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIDs})
	if err != nil {
		return nil, fmt.Errorf("classic describe groups request: %w", err)
	}

	groupInfos := make([]consumerGroupInfo, 0, len(response.Groups))
	var describeErrors []error

	for _, group := range response.Groups {
		if group == nil {
			describeErrors = append(describeErrors, errors.New("classic describe groups returned an empty group description"))
			continue
		}

		if group.Err != sarama.ErrNoError {
			describeErrors = append(describeErrors, fmt.Errorf("cannot describe classic consumer group %q: %w", group.GroupId, group.Err))
			continue
		}

		if group.State == "Dead" {
			describeErrors = append(describeErrors, fmt.Errorf("classic consumer group %q is dead", group.GroupId))
			continue
		}

		groupInfo, err := classicGroupInfoFromDescription(group)
		groupInfos = append(groupInfos, groupInfo)
		if err != nil {
			describeErrors = append(describeErrors, err)
		}
	}

	return groupInfos, errors.Join(describeErrors...)
}

func classicGroupInfoFromDescription(group *sarama.GroupDescription) (consumerGroupInfo, error) {
	groupInfo := consumerGroupInfo{
		id:                 group.GroupId,
		memberCount:        len(group.Members),
		currentAssignments: make(map[string]map[int32]struct{}),
	}
	var assignmentErrors []error

	for memberID, member := range group.Members {
		if member == nil {
			assignmentErrors = append(assignmentErrors, fmt.Errorf("consumer group %q member %q has no description", group.GroupId, memberID))
			continue
		}

		if len(member.MemberAssignment) == 0 {
			klog.Warningf("MemberAssignment is empty for group member: %v in group: %v", member.MemberId, group.GroupId)
			continue
		}

		assignment, err := member.GetMemberAssignment()
		if err != nil {
			assignmentErrors = append(assignmentErrors, fmt.Errorf("decode assignment for consumer group %q member %q: %w", group.GroupId, memberID, err))
			continue
		}

		if assignment == nil {
			continue
		}

		for topic, partitions := range assignment.Topics {
			groupInfo.addCurrentAssignment(topic, partitions)
		}
	}

	return groupInfo, errors.Join(assignmentErrors...)
}

func groupOffsetPartitions(group consumerGroupInfo, offsetMap map[string]map[int32]int64, showAll bool) map[string][]int32 {
	partitions := make(map[string][]int32)

	if showAll {
		for topic, topicOffsets := range offsetMap {
			for partition := range topicOffsets {
				partitions[topic] = append(partitions[topic], partition)
			}
		}
	} else {
		for topic, topicPartitions := range group.currentAssignments {
			for partition := range topicPartitions {
				partitions[topic] = append(partitions[topic], partition)
			}
		}
	}

	for topic := range partitions {
		sort.Slice(partitions[topic], func(i, j int) bool {
			return partitions[topic][i] < partitions[topic][j]
		})
	}

	return partitions
}
