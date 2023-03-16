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

import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

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
     * The assignment epoch. An assignment epoch smaller than the group epoch means
     * that a new assignment is required. The assignment epoch is updated when a new
     * assignment is installed.
     */
    private final TimelineInteger assignmentEpoch;

    /**
     * The group members.
     */
    private final TimelineHashMap<String, ConsumerGroupMember> members;

    /**
     * The metadata of the subscribed topics.
     */
    private final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        Objects.requireNonNull(snapshotRegistry);
        Objects.requireNonNull(groupId);

        this.snapshotRegistry = snapshotRegistry;
        this.groupId = groupId;
        this.state = new TimelineObject<>(snapshotRegistry, ConsumerGroupState.EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.assignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
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

    public ConsumerGroupMember member(
        String memberId,
        boolean createIfNotExists
    ) {
        if (memberId.isEmpty() && createIfNotExists) {
            memberId = UUID.randomUUID().toString();
        }

        ConsumerGroupMember member = members.get(memberId);
        if (member == null) {
            if (!createIfNotExists) {
                throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                    memberId, groupId));
            }
            member = new ConsumerGroupMember(snapshotRegistry, memberId);
            members.put(memberId, member);
        }

        return member;
    }

    public Map<String, ConsumerGroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    public Optional<String> preferredServerAssignor(
        String updatedMemberId,
        ConsumerGroupMemberSubscription updatedConsumerGroupMemberSubscription
    ) {
        Map<String, Integer> counts = new HashMap<>();

        if (updatedConsumerGroupMemberSubscription != null && !updatedConsumerGroupMemberSubscription.serverAssignorName().isEmpty()) {
            counts.put(updatedConsumerGroupMemberSubscription.serverAssignorName(), 1);
        }

        members.forEach((memberId, member) -> {
            ConsumerGroupMemberSubscription subscription = member.subscription();
            if (!memberId.equals(updatedMemberId) && subscription != null && !subscription.serverAssignorName().isEmpty()) {
                counts.compute(subscription.serverAssignorName(), (k, v) -> v == null ? 1 : v + 1);
            }
        });

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    public Map<String, TopicMetadata> subscriptionMetadata() {
        return Collections.unmodifiableMap(subscribedTopicMetadata);
    }

    public Map<String, TopicMetadata> updateSubscriptionMetadata(
        String updatedMemberId,
        ConsumerGroupMemberSubscription updatedConsumerGroupMemberSubscription,
        TopicsImage topicsImage
    ) {
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(subscriptionMetadata().size());

        Consumer<ConsumerGroupMemberSubscription> updateSubscription = (subscription) -> {
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

        if (updatedConsumerGroupMemberSubscription != null) {
            updateSubscription.accept(updatedConsumerGroupMemberSubscription);
        }

        members.forEach((memberId, member) -> {
            if (!updatedMemberId.equals(memberId)) {
                updateSubscription.accept(member.subscription());
            }
        });

        return newSubscriptionMetadata;
    }
}
