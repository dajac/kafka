/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.RecordBuilders.newCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newGroupEpochRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newTargetAssignmentTombstoneRecord;

public class GroupCoordinatorStateMachine {

    private final int partitionId;

    private final LogContext logContext;

    private final Logger log;

    private final SnapshotRegistry snapshotRegistry;

    private final LinkedHashMap<String, PartitionAssignor> assignors;

    private final TimelineHashMap<String, ConsumerGroup> groups;

    private MetadataImage image = MetadataImage.EMPTY;

    public GroupCoordinatorStateMachine(
        int partitionId,
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        LinkedHashMap<String, PartitionAssignor> assignors
    ) {
        this.partitionId = partitionId;
        this.logContext = logContext;
        this.log = logContext.logger(GroupCoordinatorStateMachine.class);
        this.snapshotRegistry = snapshotRegistry;
        this.assignors = assignors;
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public Result<ConsumerGroupHeartbeatResponseData> handleConsumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        validateConsumerGroupHeartbeatRequest(request);

        if (request.memberEpoch() == -1) {
            return removeMember(
                request.groupId(),
                request.memberId(),
                request.memberEpoch()
            );
        } else {
            return addOrUpdateMember(
                context,
                request
            );
        }
    }

    // TODO Add image changes handler.

    private ConsumerGroup consumerGroup(
        String groupId,
        boolean createIfNotExists
    ) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        } else {
            group = new ConsumerGroup(snapshotRegistry, groupId);
            groups.put(groupId, group);
        }

        return group;
    }

    private void validateConsumerGroupHeartbeatRequest(
        ConsumerGroupHeartbeatRequestData request
    ) throws InvalidRequestException {
        if (request.groupId() == null || request.groupId().isEmpty()) {
            throw new InvalidRequestException("GroupId can't be null or empty.");
        }

        if (request.memberId() == null) {
            throw new InvalidRequestException("MemberId can't by null.");
        }

        if (request.memberId().isEmpty() && request.memberEpoch() != 0) {
            throw new InvalidRequestException("MemberId can't be empty.");
        }

        if (request.memberEpoch() == 0 && request.instanceId() == null) {
            throw new InvalidRequestException("InstanceId must be provided in first request.");
        }

        if (request.memberEpoch() == 0 && request.rackId() == null) {
            throw new InvalidRequestException("RackId must be provided in first request.");
        }

        if (request.memberEpoch() == 0 && request.rebalanceTimeoutMs() == -1) {
            throw new InvalidRequestException("RebalanceTimeoutMs must in first request.");
        }

        if (request.memberEpoch() == 0 && request.topicPartitions() == null) {
            throw new InvalidRequestException("TopicPartitions must be set in request with epoch 0.");
        }

        if (request.memberEpoch() == 0 && !request.topicPartitions().isEmpty()) {
            throw new InvalidRequestException("TopicPartitions must be empty when re-joining.");
        }

        boolean hasSubscribedTopicNames = request.subscribedTopicNames() != null && !request.subscribedTopicNames().isEmpty();
        boolean hasSubscribedTopicRegex = request.subscribedTopicRegex() != null && !request.subscribedTopicRegex().isEmpty();
        if (request.memberEpoch() == 0 && !hasSubscribedTopicNames && !hasSubscribedTopicRegex) {
            throw new InvalidRequestException("SubscribedTopicNames or SubscribedTopicRegex must be provided in the first request.");
        }

        boolean hasServerAssignor = request.serverAssignor() != null && !request.serverAssignor().isEmpty();
        boolean hasClientAssignors = request.clientAssignors() != null && !request.clientAssignors().isEmpty();
        if (request.memberEpoch() == 0 && !hasClientAssignors && !hasServerAssignor) {
            throw new InvalidRequestException("ServerAssignor or ClientAssignors must be provided in the first request.");
        }
        if (hasClientAssignors && hasServerAssignor) {
            throw new InvalidRequestException("ServerAssignor and ClientAssignors Client can't be used together.");
        }
        if (hasServerAssignor && !assignors.containsKey(request.serverAssignor())) {
            throw new InvalidRequestException("ServerAssignor " + request.serverAssignor() + " is not supported.");
        }
        if (hasClientAssignors) {
            request.clientAssignors().forEach(clientAssignor -> {
                if (clientAssignor.name() == null || clientAssignor.name().isEmpty()) {
                    throw new InvalidRequestException("AssignorName can't by null or empty.");
                }

                if (clientAssignor.minimumVersion() < -1) {
                    throw new InvalidRequestException("Assignor " + clientAssignor.name() +
                        " must have a minimum version greater than or equals to -1.");
                }

                if (clientAssignor.maximumVersion() < 0) {
                    throw new InvalidRequestException("Assignor " + clientAssignor.name() +
                        " must have a maximum version greater than or equals to zero.");
                }

                if (clientAssignor.maximumVersion() < clientAssignor.maximumVersion()) {
                    throw new InvalidRequestException("Assignor " + clientAssignor.name() +
                        " must have a maximum version greater than or equals to the minimum version.");
                }

                if (clientAssignor.metadataVersion() < clientAssignor.minimumVersion()
                    || clientAssignor.metadataVersion() > clientAssignor.maximumVersion()) {
                    throw new InvalidRequestException("Assignor " + clientAssignor.name() +
                        " must have a version in the min-max range.");
                }
            });
        }
    }

    /**
     * Add or update member.
     */
    private Result<ConsumerGroupHeartbeatResponseData> addOrUpdateMember(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        // We have a few cases to consider here.
        // 1. The member joins: epoch == 0, memberId is empty.
        // 2. The member heartbeats: epoch > 0, memberId exists.

        List<Record> records = new ArrayList<>();
        String groupId = request.groupId();

        boolean createIfNotExists = request.memberEpoch() == 0;
        ConsumerGroup group = consumerGroup(request.groupId(), createIfNotExists);
        ConsumerGroupMember member = group.member(request.memberId(), createIfNotExists);

        if (request.memberEpoch() != member.memberEpoch()) {
            return fenceMember(groupId, member.memberId());
        }

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        ConsumerGroupMemberSubscription subscription = member.subscription().maybeUpdateWith(
            request.instanceId(),
            request.rackId(),
            request.rebalanceTimeoutMs(),
            context.clientId(),
            context.clientAddress.toString(),
            request.subscribedTopicNames(),
            request.subscribedTopicRegex(),
            request.serverAssignor(),
            request.clientAssignors() == null ? null : request.clientAssignors().stream().map(assignor -> new AssignorState(
                assignor.name(),
                assignor.reason(),
                assignor.minimumVersion(),
                assignor.maximumVersion(),
                assignor.metadataVersion(),
                ByteBuffer.wrap(assignor.metadataBytes())
            )).collect(Collectors.toList())
        );

        if (!subscription.equals(member.subscription())) {
            // Bump the group epoch.
            groupEpoch += 1;

            // Add a record for the new or updated subscription.
            records.add(newMemberSubscriptionRecord(
                groupId,
                member.memberId(),
                subscription
            ));

            // Metadata.
            subscriptionMetadata = group.updateSubscriptionMetadata(
                member.memberId(),
                subscription,
                image.topics()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                records.add(newGroupSubscriptionMetadataRecord(
                    groupId,
                    subscriptionMetadata
                ));
            }

            // Add a record for the new group epoch.
            records.add(newGroupEpochRecord(
                groupId,
                groupEpoch
            ));
        }

        // Update target assignment if needed. We don't rely on the previous step here to catch
        // the case where a new assignment would be required from a different path.
        int targetAssignmentEpoch = group.assignmentEpoch();
        ConsumerGroupMemberAssignment targetAssignment = member.targetAssignment();
        if (groupEpoch < targetAssignmentEpoch) {
            String assignorName = group.preferredServerAssignor(
                member.memberId(),
                subscription
            ).orElse(assignors.keySet().iterator().next());

            Map<String, ConsumerGroupMemberAssignment> newAssignments = new TargetAssignmentUpdater()
                .withMembers(group.members())
                .withGroupId(groupId)
                .withGroupEpoch(groupEpoch)
                .withRecordCollector(records::add)
                .withSubscriptionMetadata(subscriptionMetadata)
                .withPartitionAssignor(assignors.get(assignorName))
                .updateMemberSubscription(member.memberId(), subscription)
                .compute();

            targetAssignment = newAssignments.get(member.memberId());
            targetAssignmentEpoch = groupEpoch;
        }

        // Reconcile...
        ConsumerGroupMemberAssignment currentAssignment = member.currentAssignment();
        ConsumerGroupMemberReconciledAssignment currentReconciledAssignment = member.reconciledAssignment(targetAssignmentEpoch);
        ConsumerGroupMemberReconciledAssignment nextReconciledAssignment = currentReconciledAssignment.computeNextState(
            member.memberEpoch(),
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            request,
            (topicId, partitionId) -> true // TODO Solve this.
        );

        member.maybeUpdateReconciliationAssignment(nextReconciledAssignment);

        if (currentReconciledAssignment.memberEpoch() != nextReconciledAssignment.memberEpoch()) {
            records.add(newCurrentAssignmentRecord(
                groupId,
                member.memberId(),
                nextReconciledAssignment.memberEpoch(),
                targetAssignment
            ));
        }

        ConsumerGroupHeartbeatResponseData responseData = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(member.memberId())
            .setMemberEpoch(nextReconciledAssignment.memberEpoch())
            .setHeartbeatIntervalMs(5000); // TODO Get from config.

        if (request.topicPartitions() != null
            || request.memberEpoch() == 0
            || currentReconciledAssignment != nextReconciledAssignment) {
            ConsumerGroupHeartbeatResponseData.Assignment assignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setAssignedTopicPartitions(nextReconciledAssignment.assigned().entrySet().stream()
                    .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                        .setTopicId(keyValue.getKey())
                        .setPartitions(new ArrayList<>(keyValue.getValue())))
                    .collect(Collectors.toList()))
                .setPendingTopicPartitions(nextReconciledAssignment.pending().entrySet().stream()
                    .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                        .setTopicId(keyValue.getKey())
                        .setPartitions(new ArrayList<>(keyValue.getValue())))
                    .collect(Collectors.toList()));
            responseData.setAssignment(assignment);
        } else {
            responseData.setAssignment(null);
        }

        return new Result<>(
            records,
            responseData
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> fenceMember(
        String groupId,
        String memberId
    ) {
        return removeMember(
            groupId,
            memberId,
            -1,
            Errors.FENCED_MEMBER_EPOCH
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> removeMember(
        String groupId,
        String memberId,
        int memberEpoch
    ) {
        return removeMember(
            groupId,
            memberId,
            memberEpoch,
            Errors.NONE
        );
    }

    /**
     * Remove member from consumer group.
     */
    private Result<ConsumerGroupHeartbeatResponseData> removeMember(
        String groupId,
        String memberId,
        int memberEpoch,
        Errors error
    ) {
        ConsumerGroup group = consumerGroup(groupId, false);
        ConsumerGroupMember member = group.member(memberId, false);

        List<Record> records = new ArrayList<>();

        // Delete member current assignment.
        records.add(newCurrentAssignmentTombstoneRecord(
            groupId,
            memberId
        ));

        // Delete member target assignment.
        records.add(newTargetAssignmentTombstoneRecord(
            groupId,
            memberId
        ));

        // Delete member subscription.
        records.add(newMemberSubscriptionTombstoneRecord(
            groupId,
            memberId
        ));

        // Update subscription metadata.
        Map<String, TopicMetadata> subscriptionMetadata = group.updateSubscriptionMetadata(
            memberId,
            null,
            image.topics()
        );

        if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
            records.add(newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
        }

        // Bump group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newGroupEpochRecord(
            groupId,
            groupEpoch
        ));

        // Update target assignment.
        String assignorName = group.preferredServerAssignor(
            member.memberId(),
            null
        ).orElse(assignors.keySet().iterator().next());

        new TargetAssignmentUpdater()
            .withMembers(group.members())
            .withGroupId(groupId)
            .withGroupEpoch(groupEpoch)
            .withRecordCollector(records::add)
            .withSubscriptionMetadata(subscriptionMetadata)
            .withPartitionAssignor(assignors.get(assignorName))
            .removeMemberSubscription(member.memberId())
            .compute();

        return new Result<>(records, new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setErrorCode(error.code())
        );
    }

    public void replay(Record record) {
        // TODO
    }
}
