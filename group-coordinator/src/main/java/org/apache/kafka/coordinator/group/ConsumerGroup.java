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
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Objects;

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
    private final TimelineHashMap<Uuid, TopicMetadata> subscribedTopicMetadata;

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
    }

}
