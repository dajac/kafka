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
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentTopicMetadata;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.server.util.TranslatedValueMapView;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.kafka.coordinator.group.RecordBuilders.newTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.RecordBuilders.newTargetAssignmentRecord;

public class TargetAssignmentUpdater {
    private Consumer<Record> recordCollector = record -> { };
    private Map<String, ConsumerGroupMember> members;
    private Map<String, TopicMetadata> subscriptionMetadata;
    private String groupId;
    private int groupEpoch = -1;
    private PartitionAssignor assignor;
    private Map<String, ConsumerGroupMemberSubscription> updatedSubscriptions = new HashMap<>();

    public TargetAssignmentUpdater withPartitionAssignor(
        PartitionAssignor assignor
    ) {
        this.assignor = assignor;
        return this;
    }

    public TargetAssignmentUpdater withRecordCollector(
        Consumer<Record> recordCollector
    ) {
        this.recordCollector = recordCollector;
        return this;
    }

    public TargetAssignmentUpdater withMembers(
        Map<String, ConsumerGroupMember> members
    ) {
        this.members = members;
        return this;
    }

    public TargetAssignmentUpdater withSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return this;
    }

    public TargetAssignmentUpdater withGroupId(
        String groupId
    ) {
        this.groupId = groupId;
        return this;
    }

    public TargetAssignmentUpdater withGroupEpoch(
        int groupEpoch
    ) {
        this.groupEpoch = groupEpoch;
        return this;
    }

    public TargetAssignmentUpdater updateMemberSubscription(
        String memberId,
        ConsumerGroupMemberSubscription subscription
    ) {
        this.updatedSubscriptions.put(memberId, subscription);
        return this;
    }

    public TargetAssignmentUpdater removeMemberSubscription(
        String memberId
    ) {
        return updateMemberSubscription(memberId, null);
    }

    private void validate() {
        if (members == null) {
            throw new IllegalArgumentException("Members must be provided.");
        }
        if (subscriptionMetadata == null) {
            throw new IllegalArgumentException("Subscription metadata must be provided.");
        }
        if (groupId == null) {
            throw new IllegalArgumentException("Group id must be provided.");
        }
        if (groupEpoch < 0) {
            throw new IllegalArgumentException("Group epoch must be provided.");
        }
        if (assignor == null) {
            throw new IllegalArgumentException("Assignor must be provided.");
        }
    }

    private void addMemberSpec(
        Map<String, AssignmentMemberSpec> members,
        String memberId,
        ConsumerGroupMemberSubscription subscription,
        ConsumerGroupMemberAssignment targetAssignment
    ) {
        Set<Uuid> subscribedTopics = new HashSet<>();

        subscription.subscribedTopicNames().forEach(topicName -> {
            TopicMetadata topicMetadata = subscriptionMetadata.get(topicName);
            if (topicMetadata != null) {
                subscribedTopics.add(topicMetadata.id());
            }
        });

        members.put(memberId, new AssignmentMemberSpec(
            subscription.instanceId().isEmpty() ? Optional.empty() : Optional.of(subscription.instanceId()),
            subscription.rackId().isEmpty() ? Optional.empty() : Optional.of(subscription.rackId()),
            subscribedTopics,
            targetAssignment.partitions()
        ));
    }

    public Map<String, ConsumerGroupMemberAssignment> compute() throws PartitionAssignorException {
        validate();

        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        members.forEach((memberId, member) -> {
            addMemberSpec(
                memberSpecs,
                memberId,
                member.subscription(),
                member.targetAssignment()
            );
        });

        updatedSubscriptions.forEach((memberId, updatedSubscriptionsOrNull) -> {
            if (updatedSubscriptionsOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                addMemberSpec(
                    memberSpecs,
                    memberId,
                    updatedSubscriptionsOrNull,
                    members.get(memberId).targetAssignment()
                );
            }
        });

        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        subscriptionMetadata.forEach((topicName, topicMetadata) -> {
            topics.put(topicMetadata.id(), new AssignmentTopicMetadata(topicMetadata.numPartitions()));
        });

        // Compute the assignment.
        GroupAssignment assignment = assignor.assign(new AssignmentSpec(
            Collections.unmodifiableMap(memberSpecs),
            Collections.unmodifiableMap(topics)
        ));

        Map<String, ConsumerGroupMemberAssignment> translatedMemberAssignments = new TranslatedValueMapView<>(
            assignment.members,
            memberAssignment -> new ConsumerGroupMemberAssignment(
                (byte) 0,
                memberAssignment.targetPartitions,
                VersionedMetadata.EMPTY
            )
        );

        // Compute delta.
        members.keySet().forEach(memberId -> {
            ConsumerGroupMemberAssignment currentAssignment = members.get(memberId).targetAssignment();
            ConsumerGroupMemberAssignment newAssignment = translatedMemberAssignments.get(memberId);

            if (newAssignment != null) {
                // If we have an assignment for the member, we compare it with the previous one. If
                // it is different, we write a record to the log. Otherwise, we don't.
                if (!newAssignment.equals(currentAssignment)) {
                    recordCollector.accept(newTargetAssignmentRecord(
                        groupId,
                        memberId,
                        newAssignment.partitions()
                    ));
                }
            } else {
                // If we don't have an assignment for the member, we write an empty one to the log.
                recordCollector.accept(newTargetAssignmentRecord(
                    groupId,
                    memberId,
                    Collections.emptyMap()
                ));
            }
        });

        // Bump the assignment epoch.
        recordCollector.accept(newTargetAssignmentEpochRecord(groupId, groupEpoch));

        return translatedMemberAssignments;
    }
}
