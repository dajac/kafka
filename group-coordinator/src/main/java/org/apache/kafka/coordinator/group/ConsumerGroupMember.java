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

import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Objects;

public class ConsumerGroupMember {

    private final String memberId;

    private final TimelineInteger memberEpoch;

    private final TimelineObject<ConsumerGroupMemberSubscription> subscription;

    private final TimelineObject<ConsumerGroupMemberAssignment> targetAssignment;

    private final TimelineObject<ConsumerGroupMemberAssignment> currentAssignment;

    private final TimelineObject<ConsumerGroupMemberReconciledAssignment> reconciledAssignment;

    public ConsumerGroupMember(
        SnapshotRegistry snapshotRegistry,
        String memberId
    ) {
        Objects.requireNonNull(snapshotRegistry);
        Objects.requireNonNull(memberId);

        this.memberId = memberId;
        this.memberEpoch = new TimelineInteger(snapshotRegistry);
        this.subscription = new TimelineObject<>(snapshotRegistry, ConsumerGroupMemberSubscription.EMPTY);
        this.targetAssignment = new TimelineObject<>(snapshotRegistry, ConsumerGroupMemberAssignment.EMPTY);
        this.currentAssignment = new TimelineObject<>(snapshotRegistry, ConsumerGroupMemberAssignment.EMPTY);
        this.reconciledAssignment = new TimelineObject<>(snapshotRegistry, ConsumerGroupMemberReconciledAssignment.UNDEFINED);
    }

    public String memberId() {
        return memberId;
    }

    public int memberEpoch() {
        return memberEpoch.get();
    }

    public ConsumerGroupMemberSubscription subscription() {
        return subscription.get();
    }

    public ConsumerGroupMemberAssignment targetAssignment() {
        return targetAssignment.get();
    }

    public ConsumerGroupMemberAssignment currentAssignment() {
        return currentAssignment.get();
    }

    public ConsumerGroupMemberReconciledAssignment reconciledAssignment(){
        return reconciledAssignment.get();
    }

    public void maybeUpdateReconciliationAssignment(
        ConsumerGroupMemberReconciledAssignment newAssignment
    ) {
        ConsumerGroupMemberReconciledAssignment currentAssignment = reconciledAssignment.get();
        // Only update if the new assignment is not the current one.
        if (newAssignment != currentAssignment) {
            reconciledAssignment.set(currentAssignment);
        }
    }

    public void setSubscription(ConsumerGroupMemberSubscription subscription) {
        this.subscription.set(subscription);
    }

    public void setTargetAssignment(ConsumerGroupMemberAssignment assignment) {
        this.targetAssignment.set(assignment);
    }

    public void setCurrentAssignment(ConsumerGroupMemberAssignment assignment) {
        this.currentAssignment.set(assignment);
    }

    public void setMemberEpoch(int memberEpoch) {
        this.memberEpoch.set(memberEpoch);
    }

    @Override
    public String toString() {
        return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", subscription=" + subscription +
            ", targetAssignment=" + targetAssignment +
            ", currentAssignment=" + currentAssignment +
            ", reconciledAssignment=" + reconciledAssignment +
            ')';
    }
}
