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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class ConsumerGroupMemberReconciledAssignment {

    public static ConsumerGroupMemberReconciledAssignment UNINITIALIZED = new ConsumerGroupMemberReconciledAssignment(
        ReconciliationState.UNINITIALIZED,
        0,
        0,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    public enum ReconciliationState {
        UNINITIALIZED,
        REVOKE,
        ASSIGN,
        STABLE
    }

    private final ReconciliationState state;

    private final int memberEpoch;

    private final int assignmentEpoch;

    private final Map<Uuid, Set<Integer>> assigned;

    private final Map<Uuid, Set<Integer>> revoking;

    private final Map<Uuid, Set<Integer>> pending;

    public ConsumerGroupMemberReconciledAssignment(
        ReconciliationState state,
        int memberEpoch,
        int assignmentEpoch,
        Map<Uuid, Set<Integer>> assigned,
        Map<Uuid, Set<Integer>> revoking,
        Map<Uuid, Set<Integer>> pending
    ) {
        Objects.requireNonNull(assigned);
        Objects.requireNonNull(revoking);
        Objects.requireNonNull(pending);

        this.state = state;
        this.memberEpoch = memberEpoch;
        this.assignmentEpoch = assignmentEpoch;
        this.assigned = Collections.unmodifiableMap(assigned);
        this.revoking = Collections.unmodifiableMap(revoking);
        this.pending = Collections.unmodifiableMap(pending);
    }

    public ReconciliationState state() {
        return state;
    }

    public int memberEpoch() {
        return memberEpoch;
    }

    public int assignmentEpoch() {
        return assignmentEpoch;
    }

    public Map<Uuid, Set<Integer>> assigned() {
        return assigned;
    }

    public Map<Uuid, Set<Integer>> revoking() {
        return revoking;
    }

    public Map<Uuid, Set<Integer>> pending() {
        return pending;
    }

    private ConsumerGroupMemberReconciledAssignment computeRevokeState(
        int currentMemberEpoch,
        ConsumerGroupMemberAssignment currentAssignment,
        int targetAssignmentEpoch,
        ConsumerGroupMemberAssignment targetAssignment
    ) {
        Map<Uuid, Set<Integer>> assigned = new HashMap<>();
        Map<Uuid, Set<Integer>> revoking = new HashMap<>();

        // Set of combined topic ids.
        Set<Uuid> allTopicIds = new HashSet<>(currentAssignment.partitions().keySet());
        allTopicIds.addAll(targetAssignment.partitions().keySet());

        for (Uuid topicId: allTopicIds) {
            Set<Integer> currentPartitions = currentAssignment.partitions().getOrDefault(topicId, Collections.emptySet());
            Set<Integer> targetPartitions = targetAssignment.partitions().getOrDefault(topicId, Collections.emptySet());

            if (!currentPartitions.isEmpty() && !targetPartitions.isEmpty()) {
                currentPartitions.forEach(partitionId -> {
                    if (targetPartitions.contains(partitionId)) {
                        addPartitionToAssignment(assigned, topicId, partitionId);
                    } else {
                        addPartitionToAssignment(revoking, topicId, partitionId);
                    }
                });
            } else if (targetPartitions.isEmpty()) {
                revoking.put(topicId, currentPartitions);
            }
        }

        return new ConsumerGroupMemberReconciledAssignment(
            ConsumerGroupMemberReconciledAssignment.ReconciliationState.REVOKE,
            currentMemberEpoch,
            targetAssignmentEpoch,
            assigned,
            revoking,
            Collections.emptyMap()
        );
    }

    private ConsumerGroupMemberReconciledAssignment computeAssignOrStableState(
        int targetAssignmentEpoch,
        ConsumerGroupMemberAssignment targetAssignment,
        BiFunction<Uuid, Integer, Boolean> isFree
    ) {
        Map<Uuid, Set<Integer>> assigned = new HashMap<>();
        Map<Uuid, Set<Integer>> pending = new HashMap<>();

        targetAssignment.partitions().forEach((topicId, targetPartitions) -> {
            targetPartitions.forEach(partitionId -> {
                if (isFree.apply(topicId, partitionId)) {
                    addPartitionToAssignment(assigned, topicId, partitionId);
                } else {
                    addPartitionToAssignment(pending, topicId, partitionId);
                }
            });
        });

        ConsumerGroupMemberReconciledAssignment.ReconciliationState state;
        if (pending.isEmpty()) {
            state = ConsumerGroupMemberReconciledAssignment.ReconciliationState.STABLE;
        } else {
            state = ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN;
        }

        return new ConsumerGroupMemberReconciledAssignment(
            state,
            targetAssignmentEpoch,
            targetAssignmentEpoch,
            assigned,
            Collections.emptyMap(),
            assigned
        );
    }

    private ConsumerGroupMemberReconciledAssignment checkPendingPartitions(
        int targetAssignmentEpoch,
        ConsumerGroupMemberAssignment targetAssignment,
        BiFunction<Uuid, Integer, Boolean> isFree
    ) {
        Map<Uuid, Set<Integer>> newPending = new HashMap<>();
        boolean hasChanged = false;

        for (Map.Entry<Uuid, Set<Integer>> entry: pending.entrySet()) {
            Uuid topicId = entry.getKey();
            Set<Integer> partitions = entry.getValue();

            for (Integer partitionId : partitions) {
                if (isFree.apply(topicId, partitionId)) {
                    hasChanged = true;
                } else {
                    addPartitionToAssignment(newPending, topicId, partitionId);
                }
            }
        }

        if (hasChanged) {
            Map<Uuid, Set<Integer>> newAssigned = new HashMap<>();
            targetAssignment.partitions().forEach((topicId, targetPartitions) -> {
                targetPartitions.forEach(partitionId -> {
                    if (isFree.apply(topicId, partitionId)) {
                        addPartitionToAssignment(newAssigned, topicId, partitionId);
                    }
                });
            });

            ConsumerGroupMemberReconciledAssignment.ReconciliationState state;
            if (newPending.isEmpty()) {
                state = ConsumerGroupMemberReconciledAssignment.ReconciliationState.STABLE;
            } else {
                state = ConsumerGroupMemberReconciledAssignment.ReconciliationState.ASSIGN;
            }

            return new ConsumerGroupMemberReconciledAssignment(
                state,
                memberEpoch,
                targetAssignmentEpoch,
                newAssigned,
                Collections.emptyMap(),
                newPending
            );
        } else {
            return this;
        }
    }

    private void addPartitionToAssignment(
        Map<Uuid, Set<Integer>> assignment,
        Uuid topicId,
        int partitionId
    ) {
        assignment.compute(topicId, (__, partitionsOrNull) -> {
            Set<Integer> partitions = partitionsOrNull;
            if (partitions == null) partitions = new HashSet<>();
            partitions.add(partitionId);
            return partitions;
        });
    }

    private boolean hasRevokedAllPartitions(
        ConsumerGroupHeartbeatRequestData request
    ) {
        if (request.topicPartitions() == null) return false;
        if (request.topicPartitions().size() != assigned.size()) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : request.topicPartitions()) {
            Set<Integer> partitions = assigned.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    public ConsumerGroupMemberReconciledAssignment computeNextState(
        int currentMemberEpoch,
        ConsumerGroupMemberAssignment currentAssignment,
        int targetAssignmentEpoch,
        ConsumerGroupMemberAssignment targetAssignment,
        ConsumerGroupHeartbeatRequestData request,
        BiFunction<Uuid, Integer, Boolean> isFree
    ) {
        switch (state) {
            case UNINITIALIZED:
                if (currentMemberEpoch < targetAssignmentEpoch) {
                    return computeRevokeState(
                        currentMemberEpoch,
                        currentAssignment,
                        targetAssignmentEpoch,
                        targetAssignment
                    ).computeNextState(
                        currentMemberEpoch,
                        currentAssignment,
                        targetAssignmentEpoch,
                        targetAssignment,
                        request,
                        isFree
                    );
                } else {
                    return computeAssignOrStableState(
                        targetAssignmentEpoch,
                        targetAssignment,
                        isFree
                    );
                }

            case REVOKE:
                if (revoking.isEmpty() || hasRevokedAllPartitions(request)) {
                    return computeAssignOrStableState(
                        targetAssignmentEpoch,
                        targetAssignment,
                        isFree
                    );
                } else {
                    return this;
                }

            case ASSIGN:
                return checkPendingPartitions(
                    targetAssignmentEpoch,
                    targetAssignment,
                    isFree
                );

            case STABLE:
            default:
                return this;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroupMemberReconciledAssignment that = (ConsumerGroupMemberReconciledAssignment) o;

        if (memberEpoch != that.memberEpoch) return false;
        if (assignmentEpoch != that.assignmentEpoch) return false;
        if (state != that.state) return false;
        if (!Objects.equals(assigned, that.assigned)) return false;
        if (!Objects.equals(revoking, that.revoking)) return false;
        return Objects.equals(pending, that.pending);
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + memberEpoch;
        result = 31 * result + assignmentEpoch;
        result = 31 * result + (assigned != null ? assigned.hashCode() : 0);
        result = 31 * result + (revoking != null ? revoking.hashCode() : 0);
        result = 31 * result + (pending != null ? pending.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerGroupMemberReconciledAssignment(" +
            "state=" + state +
            ", memberEpoch=" + memberEpoch +
            ", assignmentEpoch=" + assignmentEpoch +
            ", assigned=" + assigned +
            ", revoking=" + revoking +
            ", pending=" + pending +
            ')';
    }
}
