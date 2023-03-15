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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentTopicMetadata;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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

    private ConsumerGroup getAndMaybeCreateConsumerGroup(String groupId) {
        return groups.computeIfAbsent(groupId, __ -> new ConsumerGroup(snapshotRegistry, groupId));
    }

    private ConsumerGroup getConsumerGroupOrThrow(String groupId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    private Record newMemberSubscriptionRecord(
        String groupId,
        String memberId,
        MemberSubscription newMemberSubscription
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 5
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setRackId(newMemberSubscription.rackId())
                    .setInstanceId(newMemberSubscription.instanceId())
                    .setClientId(newMemberSubscription.clientId())
                    .setClientHost(newMemberSubscription.clientHost())
                    .setSubscribedTopicNames(newMemberSubscription.subscribedTopicNames())
                    .setSubscribedTopicRegex(newMemberSubscription.subscribedTopicRegex())
                    .setServerAssignor(newMemberSubscription.serverAssignorName())
                    .setRebalanceTimeoutMs(newMemberSubscription.rebalanceTimeoutMs())
                    .setAssignors(newMemberSubscription.assignorStates().stream().map(assignorState ->
                        new ConsumerGroupMemberMetadataValue.Assignor()
                            .setName(assignorState.name())
                            .setReason(assignorState.reason())
                            .setMinimumVersion(assignorState.minimumVersion())
                            .setMaximumVersion(assignorState.maximumVersion())
                            .setVersion(assignorState.metadataVersion())
                            .setMetadata(assignorState.metadataBytes().array())
                    ).collect(Collectors.toList())),
                (short) 0
            )
        );
    }

    private Record newMemberSubscriptionTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 5
            ),
            null
        );
    }

    private Record newGroupSubscriptionMetadataRecord(
        String groupId,
        Map<String, TopicMetadata> newSubscriptionMetadata
    ) {
        ConsumerGroupPartitionMetadataValue value = new ConsumerGroupPartitionMetadataValue();
        newSubscriptionMetadata.forEach((topicName, topicMetadata) ->
            value.topics().add(new ConsumerGroupPartitionMetadataValue.TopicMetadata()
               .setTopicId(topicMetadata.id())
               .setTopicName(topicMetadata.name())
               .setNumPartitions(topicMetadata.numPartitions())
            )
        );

        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId(groupId),
                (short) 4
            ),
            new ApiMessageAndVersion(
                value,
                (short) 0
            )
        );
    }

    private Record newGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId(groupId),
                (short) 3
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(newGroupEpoch),
                (short) 0
            )
        );
    }

    private Record newTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<Uuid, Set<Integer>> partitions
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 7
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(partitions.entrySet().stream()
                        .map(keyValue -> new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(keyValue.getKey())
                            .setPartitions(new ArrayList<>(keyValue.getValue())))
                        .collect(Collectors.toList())),
                (short) 0
            )
        );
    }

    private Record newTargetAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 7
            ),
            null
        );
    }

    private Record newTargetAssignmentEpochRecord(
        String groupId,
        int groupEpcoh
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                (short) 6
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(groupEpcoh),
                (short) 0
            )
        );
    }

    private Record newCurrentAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 8
            ),
            null // Tombstone
        );
    }

    private MemberTargetAssignment updateTargetAssignment(
        List<Record> records,
        ConsumerGroup group,
        String groupId,
        int groupEpoch,
        String memberId,
        MemberSubscription memberSubscription,
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        // Get the assignor to use.
        String assignorName = group.preferredServerAssignor(
            memberId,
            memberSubscription
        ).orElse(assignors.keySet().iterator().next());

        // Prepare data.
        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        BiConsumer<String, MemberSubscription> addMember = (mid, subscription) -> {
            Set<Uuid> subscribedTopics = new HashSet<>();

            subscription.subscribedTopicNames().forEach(topicName -> {
                TopicMetadata topicMetadata = subscriptionMetadata.get(topicName);
                if (topicMetadata != null) {
                    subscribedTopics.add(topicMetadata.id());
                }
            });

            members.put(mid, new AssignmentMemberSpec(
                subscription.instanceId().isEmpty() ? Optional.empty() : Optional.of(subscription.instanceId()),
                subscription.rackId().isEmpty() ? Optional.empty() : Optional.of(subscription.rackId()),
                subscribedTopics,
                group.targetAssignment(mid)
            ));
        };

        group.subscriptions().forEach(addMember);

        if (memberSubscription != null) {
            addMember.accept(memberId, memberSubscription);
        } else {
            members.remove(memberId);
        }

        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        subscriptionMetadata.forEach((topicName, topicMetadata) -> {
            topics.put(topicMetadata.id(), new AssignmentTopicMetadata(topicMetadata.numPartitions()));
        });

        // Compute the assignment.
        PartitionAssignor assignor = assignors.get(assignorName);
        // TODO: Handle failures.
        GroupAssignment assignment = assignor.assign(new AssignmentSpec(
            Collections.unmodifiableMap(members),
            Collections.unmodifiableMap(topics)
        ));

        // Compute delta.
        members.keySet().forEach(mid -> {
            MemberTargetAssignment currentAssignment = group.memberTargetAssignment(memberId);
            MemberAssignment newAssignment = assignment.members.get(memberId);

            if (newAssignment != null) {
                // If we have an assignment for the member, we compare it with the previous one. If
                // it is different, we write a record to the log. Otherwise, we don't.
                if (currentAssignment == null || !currentAssignment.assignedPartitions().equals(newAssignment.targetPartitions)) {
                    records.add(newTargetAssignmentRecord(
                        groupId,
                        memberId,
                        newAssignment.targetPartitions
                    ));
                }
            } else {
                // If we don't have an assignment for the member, we write an empty one to the log.
                records.add(newTargetAssignmentRecord(
                    groupId,
                    memberId,
                    Collections.emptyMap()
                ));
            }
        });

        // Bump the assignment epoch.
        records.add(newTargetAssignmentEpochRecord(groupId, groupEpoch));

        // Keep a reference of the new target assignment of the current member.
        MemberAssignment newTargetAssignment = assignment.members.get(memberId);
        if (newTargetAssignment != null) {
            return new MemberTargetAssignment(
                newTargetAssignment.targetPartitions,
                VersionedMetadata.EMPTY
            );
        } else {
            return new MemberTargetAssignment(
                Collections.emptyMap(),
                VersionedMetadata.EMPTY
            );
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

        String groupId = request.groupId();
        int memberEpoch = request.memberEpoch();

        String memberId;
        ConsumerGroup group;
        if (memberEpoch > 0) {
            // The group must exist if a member has an epoch greater than zero.
            group = getConsumerGroupOrThrow(groupId);

            // The member must exist as well.
            memberId = request.memberId();
            if (!group.subscriptions().containsKey(memberId)) {
                throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                    memberId, groupId));
            }

            // The member epoch should match the expected epoch.
            MemberCurrentAssignment currentAssignment = group.memberCurrentAssignment(memberId);
            // TODO: Member could rejoin with the last epoch if the response with the epoch
            // bump got lost.
            if (currentAssignment.epoch() != memberEpoch) {
                return fenceMember(
                    groupId,
                    memberId,
                    memberEpoch
                );
            }
        } else {
            // The group may not exist yet if the member an epoch equals to zero.
            // In this case, we create an empty group.
            group = getAndMaybeCreateConsumerGroup(groupId);

            // A unique identifier is generated for the member. Otherwise, the provided
            // member id is used.
            memberId = request.memberId().isEmpty() ? Uuid.randomUuid().toString() : request.memberId();
        }

        int groupEpoch = group.groupEpoch();
        List<Record> records = new ArrayList<>();
        ConsumerGroupHeartbeatResponseData responseData = new ConsumerGroupHeartbeatResponseData();

        // Create or update the member subscription. This does not update the group state but only
        // returns an updated MemberSubscription.
        Optional<MemberSubscription> createdOrUpdatedSubscription = group.maybeCreateOrUpdateSubscription(
            memberId,
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

        final MemberSubscription memberSubscription;
        final Map<String, TopicMetadata> subscriptionMetadata;
        final MemberTargetAssignment memberTargetAssignment;

        if (createdOrUpdatedSubscription.isPresent()) {
            memberSubscription = createdOrUpdatedSubscription.get();

            // Bump the group epoch.
            groupEpoch += 1;

            // Add a record for the new or updated subscription.
            records.add(newMemberSubscriptionRecord(
                groupId,
                memberId,
                memberSubscription
            ));

            // Metadata.
            Optional<Map<String, TopicMetadata>> maybeUpdatedSubscriptionMetadata = group.maybeUpdateSubscriptionMetadata(
                memberId,
                memberSubscription,
                image.topics()
            );

            if (maybeUpdatedSubscriptionMetadata.isPresent()) {
                subscriptionMetadata = maybeUpdatedSubscriptionMetadata.get();
                records.add(newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            } else {
                subscriptionMetadata = group.subscriptionMetadata();
            }

            // Add a record for the new group epoch.
            records.add(newGroupEpochRecord(
                groupId,
                groupEpoch
            ));
        } else {
            memberSubscription = group.subscription(memberId);
            subscriptionMetadata = group.subscriptionMetadata();
        }

        // Update target assignment if needed. We don't rely on the previous step here to catch
        // the case where a new assignment would be required from a different path.
        if (groupEpoch < group.assignmentEpoch()) {
            memberTargetAssignment = updateTargetAssignment(
                records,
                group,
                groupId,
                groupEpoch,
                memberId,
                memberSubscription,
                subscriptionMetadata
            );
        } else {
            memberTargetAssignment = group.memberTargetAssignment(memberId);
        }

        // Reconcile...
        final MemberCurrentAssignment memberCurrentAssignment = group.memberCurrentAssignment(memberId);




        // Update target assignment

        // Current Epoch < Target Epoch
        // - Compute Current ^ Target.
        // - If member has Current ^ Target, moves it to next epoch with Target - Pending and
        //   returns Target - Pending in the response.
        // - Otherwise, return Current ^ Target in the response. How do we know if it was already
        //   return in the previous response? I could ack that the new target epoch was processed?
        // Current Epoch == Target Epoch
        // - Compute new target assignment: Target - Pending.
        // - If new target is different from previous target, add record to update the state.
        // - Return Target - Pending if record added.

        // How to compute Pending?
        // 1) Always store the actual assigned partition in the current assignment (instead of the full target).
        //    Whenever we update it, update the partition assignment (current owner) as well.
        // 2) Always tore the target in the current assignment (less writes). Whenever a partition is released,
        //    update its current epoch to the current target.

        // How do I know if the assignment must be sent back?
        // 1) Keep the last one sent and compare. This works for both the revocation and the assignment phase.
        // 2) Use another signal? Member state? Revoke, Revoking, Assigning, Assigned? In-memory or backed by the records?

        return new Result<>(
            records,
            responseData
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> fenceMember(
        String groupId,
        String memberId,
        int memberEpoch
    ) {
        return removeMember(
            groupId,
            memberId,
            memberEpoch,
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
        ConsumerGroup group = getConsumerGroupOrThrow(groupId);

        if (!group.subscriptions().containsKey(memberId)) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                memberId, groupId));
        }

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
        Optional<Map<String, TopicMetadata>> maybeUpdatedSubscriptionMetadata = group.maybeUpdateSubscriptionMetadata(
            memberId,
            null,
            image.topics()
        );

        final Map<String, TopicMetadata> updatedSubscriptionMetadata;
        if (maybeUpdatedSubscriptionMetadata.isPresent()) {
            updatedSubscriptionMetadata = maybeUpdatedSubscriptionMetadata.get();
            records.add(newGroupSubscriptionMetadataRecord(groupId, updatedSubscriptionMetadata));
        } else {
            updatedSubscriptionMetadata = group.subscriptionMetadata();
        }

        // Bump group epoch.
        final int groupEpoch = group.groupEpoch() + 1;
        records.add(newGroupEpochRecord(
            groupId,
            groupEpoch
        ));

        // Update target assignment.
        updateTargetAssignment(
            records,
            group,
            groupId,
            groupEpoch,
            memberId,
            null,
            updatedSubscriptionMetadata
        );

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
