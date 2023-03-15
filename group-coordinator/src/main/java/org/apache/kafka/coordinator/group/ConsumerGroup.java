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
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Holds the metadata of a consumer group.
 */
public class ConsumerGroup {

    public enum ConsumerGroupState {
        EMPTY,
        ASSIGNING,
        RECONCILING,
        STABLE,
        DEAD
    }

    /**
     * The group id.
     */
    private final String groupId;

    /**
     * The group state.
     */
    private final TimelineObject<ConsumerGroupState> state;

    /**
     * The group epoch. The epoch is incremented whenever the subscriptions
     * are updated and it will trigger the computation of a new assignment
     * for the group.
     */
    private final TimelineInteger groupEpoch;

    /**
     * The member subscriptions.
     */
    private final TimelineHashMap<String, MemberSubscription> memberSubscriptions;

    /**
     * The metadata of the subscribed topics.
     */
    private final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    /**
     * The assignment epoch. An assignment epoch smaller than the group epoch means
     * that a new assignment is required. The assignment epoch is updated when a new
     * assignment is installed.
     */
    private final TimelineInteger assignmentEpoch;

    /**
     * The target assignment for each member in the group. The target assignment represents
     * the desired state for the group and each member will eventually converge to it.
     */
    private final TimelineHashMap<String, MemberTargetAssignment> memberTargetAssignments;

    /**
     * The current assignment for each member in the group. The current assignment represents,
     * as the name suggests, the current assignment of a member. This includes the current
     * epoch and the current owned partitions.
     */
    private final TimelineHashMap<String, MemberCurrentAssignment> memberCurrentAssignments;

    /**
     * The current owner for any given partitions in the current assignment.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, String>> currentPartitionOwners;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        Objects.requireNonNull(snapshotRegistry);
        Objects.requireNonNull(groupId);

        this.groupId = groupId;
        this.state = new TimelineObject<>(snapshotRegistry, ConsumerGroupState.EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.memberSubscriptions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.assignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.memberTargetAssignments = new TimelineHashMap<>(snapshotRegistry, 0);
        this.memberCurrentAssignments = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionOwners = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Returns the current group epoch.
     */
    public int groupEpoch() {
        return groupEpoch.get();
    }

    /**
     * Returns the current assignment epoch.
     */
    public int assignmentEpoch() {
        return assignmentEpoch.get();
    }

    public Optional<String> preferredServerAssignor(
        String updatedMemberId,
        MemberSubscription updatedMemberSubscription
    ) {
        Map<String, Integer> counts = new HashMap<>();

        if (updatedMemberSubscription != null && !updatedMemberSubscription.serverAssignorName().isEmpty()) {
            counts.put(updatedMemberSubscription.serverAssignorName(), 1);
        }

        memberSubscriptions.forEach((memberId, memberSubscription) -> {
            if (!memberId.equals(updatedMemberId) && !memberSubscription.serverAssignorName().isEmpty()) {
                counts.compute(
                    memberSubscription.serverAssignorName(),
                    (k, v) -> v == null ? 1 : v + 1
                );
            }
        });

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    public Map<String, TopicMetadata> subscriptionMetadata() {
        return Collections.unmodifiableMap(subscribedTopicMetadata);
    }

    public MemberSubscription subscription(String memberId) throws UnknownMemberIdException {
        return memberSubscriptions.get(memberId);
    }

    public Map<String, MemberSubscription> subscriptions() {
        return Collections.unmodifiableMap(memberSubscriptions);
    }

    public MemberCurrentAssignment memberCurrentAssignment(String memberId){
        return memberCurrentAssignments.get(memberId);
    }

    public MemberTargetAssignment memberTargetAssignment(String memberId) {
        return memberTargetAssignments.get(memberId);
    }

    public Map<Uuid, Set<Integer>> targetAssignment(String memberId) {
        MemberTargetAssignment targetAssignment = memberTargetAssignments.get(memberId);
        if (targetAssignment != null) {
            return targetAssignment.assignedPartitions();
        } else {
            return Collections.emptyMap();
        }
    }

    public Map<Integer, String> currentPartitionOwners(Uuid topicId) {
        TimelineHashMap<Integer, String> owners = currentPartitionOwners.get(topicId);
        if (owners != null) {
            return Collections.unmodifiableMap(owners);
        } else {
            return Collections.emptyMap();
        }
    }

    public Optional<MemberSubscription> maybeCreateOrUpdateSubscription(
        String memberId,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String serverAssignorName,
        List<AssignorState> assignorStates
    ) {
        MemberSubscription currentMemberSubscription = memberSubscriptions.get(memberId);
        if (currentMemberSubscription == null) {
            return Optional.of(new MemberSubscription(
                instanceId,
                rackId,
                rebalanceTimeoutMs,
                clientId,
                clientHost,
                subscribedTopicNames,
                subscribedTopicRegex,
                serverAssignorName,
                assignorStates
            ));
        } else {
            return currentMemberSubscription.maybeUpdateWith(
                instanceId,
                rackId,
                rebalanceTimeoutMs,
                clientId,
                clientHost,
                subscribedTopicNames,
                subscribedTopicRegex,
                serverAssignorName,
                assignorStates
            );
        }
    }

    public Optional<Map<String, TopicMetadata>> maybeUpdateSubscriptionMetadata(
        String updatedMemberId,
        MemberSubscription updatedMemberSubscription,
        TopicsImage topicsImage
    ) {
        Map<String, TopicMetadata> oldSubscriptionMetadata = subscribedTopicMetadata;
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(oldSubscriptionMetadata.size());

        Consumer<MemberSubscription> updateSubscription = (subscription) -> {
            subscription.subscribedTopicNames().forEach(topicName -> {
                newSubscriptionMetadata.computeIfAbsent(topicName, __ -> {
                    TopicImage topicImage = topicsImage.getTopic(topicName);
                    if (topicImage == null) {
                        return null;
                    } else {
                        return new TopicMetadata(
                            topicImage.id(),
                            topicImage.name(),
                            topicImage.partitions().size()
                        );
                    }
                });
            });
        };

        if (updatedMemberSubscription != null) {
            updateSubscription.accept(updatedMemberSubscription);
        }

        memberSubscriptions.forEach((memberId, memberSubscription) -> {
            if (!updatedMemberId.equals(memberId)) {
                updateSubscription.accept(memberSubscription);
            }
        });

        if (newSubscriptionMetadata.equals(oldSubscriptionMetadata)) {
            return Optional.empty();
        } else {
            return Optional.of(newSubscriptionMetadata);
        }
    }
}
