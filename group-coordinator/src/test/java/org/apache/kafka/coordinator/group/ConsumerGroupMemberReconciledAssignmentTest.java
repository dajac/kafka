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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.ConsumerGroupMemberReconciledAssignment.UNDEFINED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupMemberReconciledAssignmentTest {

    private static Map.Entry<Uuid, Set<Integer>> mkTopicAssignment(
        Uuid topicId,
        Integer... partitions
    ) {
        return new AbstractMap.SimpleEntry<>(
            topicId,
            new HashSet<>(Arrays.asList(partitions))
        );
    }

    @SafeVarargs
    private static Map<Uuid, Set<Integer>> mkAssignment(Map.Entry<Uuid, Set<Integer>>... entries) {
        Map<Uuid, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<Uuid, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }

    private static ConsumerGroupHeartbeatRequestData requestFromAssignment(Map<Uuid, Set<Integer>> assignment) {
        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData();
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions = new ArrayList<>();
        request.setTopicPartitions(topicPartitions);

        assignment.forEach((topicId, partitions) -> {
            ConsumerGroupHeartbeatRequestData.TopicPartitions topic = new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                .setTopicId(topicId)
                .setPartitions(new ArrayList<>(partitions));
            topicPartitions.add(topic);
        });

        return request;
    }

    @Test
    public void testTransitionFromUndefinedToRevoke() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId3, 5, 6, 7)
            ),
            VersionedMetadata.EMPTY
        );

        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Member stays at the current epoch while revoking partitions.
        assertEquals(currentMemberEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 2, 3),
            mkTopicAssignment(topicId3, 7)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1),
            mkTopicAssignment(topicId2, 4, 5, 6),
            mkTopicAssignment(topicId3, 8, 9)
        ), next.revoking());

        assertEquals(Collections.emptyMap(), next.pending());
    }

    @Test
    public void testTransitionFromUndefinedToAssign() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        Map<Uuid, Set<Integer>> free = mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5),
            mkTopicAssignment(topicId3, 7, 8, 9)
        );

        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> free.getOrDefault(topicId, Collections.emptySet()).contains(partitionId)
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5),
            mkTopicAssignment(topicId3, 7, 8, 9)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), next.pending());

        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testTransitionFromUndefinedToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.STABLE, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6),
            mkTopicAssignment(topicId3, 7, 8, 9)
        ), next.assigned());

        assertEquals(Collections.emptyMap(), next.pending());
        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testTransitionFromRevokeToAssign() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId3, 5, 6, 7)
            ),
            VersionedMetadata.EMPTY
        );

        // Transition to Revoke state.
        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> false
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Stay in Revoke state because client did not ack.
        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData().setTopicPartitions(null),
            (topicId, partitionId) -> false
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Transition to Assign when the member acks the revocation.
        Map<Uuid, Set<Integer>> free = mkAssignment(
            mkTopicAssignment(topicId1, 2, 3),
            mkTopicAssignment(topicId3, 7)
        );

        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            requestFromAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId3, 7)
            )),
            (topicId, partitionId) -> free.getOrDefault(topicId, Collections.emptySet()).contains(partitionId)
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 2, 3),
            mkTopicAssignment(topicId3, 7)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4),
            mkTopicAssignment(topicId3, 5, 6)
        ), next.pending());

        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testTransitionFromAssignToAssign() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        Map<Uuid, Set<Integer>> free = mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        );

        // Member transitions to Assign.
        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> free.getOrDefault(topicId, Collections.emptySet()).contains(partitionId)
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6),
            mkTopicAssignment(topicId3, 7, 8, 9)
        ), next.pending());

        assertEquals(Collections.emptyMap(), next.revoking());

        // Some partitions are freed up but not all so member stays in Assign.
        Map<Uuid, Set<Integer>> updatedFree = mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5),
            mkTopicAssignment(topicId3, 7)
        );

        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> updatedFree.getOrDefault(topicId, Collections.emptySet()).contains(partitionId)
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN, next.state());

        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5),
            mkTopicAssignment(topicId3, 7)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId2, 6),
            mkTopicAssignment(topicId3, 8, 9)
        ), next.pending());

        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testTransitionFromAssignToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        Map<Uuid, Set<Integer>> free = mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        );

        // Member transitions to Assign.
        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> free.getOrDefault(topicId, Collections.emptySet()).contains(partitionId)
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6),
            mkTopicAssignment(topicId3, 7, 8, 9)
        ), next.pending());

        assertEquals(Collections.emptyMap(), next.revoking());

        // All partitions are freed up so member transitions to Stable.
        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.STABLE, next.state());

        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6),
            mkTopicAssignment(topicId3, 7, 8, 9)
        ), next.assigned());

        assertEquals(Collections.emptyMap(), next.pending());
        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testTransitionFromRevokeToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId3, 5, 6, 7)
            ),
            VersionedMetadata.EMPTY
        );

        // Transition to Revoke state.
        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> false
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Stay in Revoke state because client did not ack.
        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData().setTopicPartitions(null),
            (topicId, partitionId) -> false
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Member acks the revocation and all partitions are free so the member transitions
        // to Stable.
        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            requestFromAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId3, 7)
            )),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.STABLE, next.state());

        // Member transitions to the target epoch.
        assertEquals(targetAssignmentEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 2, 3, 4),
            mkTopicAssignment(topicId3, 5, 6, 7)
        ), next.assigned());

        assertEquals(Collections.emptyMap(), next.pending());
        assertEquals(Collections.emptyMap(), next.revoking());
    }

    @Test
    public void testNewTargetAssignmentRestartsProcess() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        int currentMemberEpoch = 1;
        ConsumerGroupMemberAssignment currentAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6),
                mkTopicAssignment(topicId3, 7, 8, 9)
            ),
            VersionedMetadata.EMPTY
        );

        int targetAssignmentEpoch = 2;
        ConsumerGroupMemberAssignment targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId3, 5, 6, 7)
            ),
            VersionedMetadata.EMPTY
        );

        ConsumerGroupMemberReconciledAssignment next = UNDEFINED.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Member stays at the current epoch while revoking partitions.
        assertEquals(currentMemberEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 2, 3),
            mkTopicAssignment(topicId3, 7)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1),
            mkTopicAssignment(topicId2, 4, 5, 6),
            mkTopicAssignment(topicId3, 8, 9)
        ), next.revoking());

        assertEquals(Collections.emptyMap(), next.pending());

        // A new target assignment is available. The process restarts.
        targetAssignmentEpoch = 3;
        targetAssignment = new ConsumerGroupMemberAssignment(
            (byte) 0,
            mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7),
                mkTopicAssignment(topicId3, 8, 9, 1)
            ),
            VersionedMetadata.EMPTY
        );

        next = next.computeNextState(
            currentMemberEpoch,
            currentAssignment,
            targetAssignmentEpoch,
            targetAssignment,
            new ConsumerGroupHeartbeatRequestData(),
            (topicId, partitionId) -> true
        );

        assertEquals(ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE, next.state());

        // Member stays at the current epoch while revoking partitions.
        assertEquals(currentMemberEpoch, next.memberEpoch());
        assertEquals(targetAssignmentEpoch, next.assignmentEpoch());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 2, 3),
            mkTopicAssignment(topicId2, 5, 6),
            mkTopicAssignment(topicId3, 8, 9)
        ), next.assigned());

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1),
            mkTopicAssignment(topicId2, 4),
            mkTopicAssignment(topicId3, 7)
        ), next.revoking());

        assertEquals(Collections.emptyMap(), next.pending());
    }
}
