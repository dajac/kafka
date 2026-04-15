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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.server.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A partition assignor that supports canary consumers. Members whose instance id
 * matches a configurable regex are considered canary members and receive at most
 * N partitions each. The remaining partitions are distributed uniformly among
 * non-canary members.
 *
 * <p>This assignor only supports homogeneous subscriptions (all members subscribed
 * to the same set of topics).
 *
 * <p>Configuration:
 * <ul>
 *     <li>{@code canary.assignor.instance.id.regex} - Regex pattern to identify canary members
 *         by their instance id. Members without an instance id never match. Required.</li>
 *     <li>{@code canary.assignor.max.partitions.per.member} - Maximum number of partitions
 *         assigned to each canary member. Default: 1.</li>
 * </ul>
 */
public class CanaryAssignor implements ConsumerGroupPartitionAssignor, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(CanaryAssignor.class);

    public static final String NAME = "canary";
    public static final String INSTANCE_ID_REGEX_CONFIG = "canary.assignor.instance.id.regex";
    public static final String MAX_PARTITIONS_PER_MEMBER_CONFIG = "canary.assignor.max.partitions.per.member";
    public static final int DEFAULT_MAX_PARTITIONS_PER_MEMBER = 1;

    private Pattern instanceIdPattern;
    private int maxPartitionsPerCanaryMember;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object regexValue = configs.get(INSTANCE_ID_REGEX_CONFIG);
        if (regexValue == null) {
            throw new ConfigException("Missing required configuration: " + INSTANCE_ID_REGEX_CONFIG);
        }
        instanceIdPattern = Pattern.compile(regexValue.toString());

        Object maxPartitionsValue = configs.get(MAX_PARTITIONS_PER_MEMBER_CONFIG);
        if (maxPartitionsValue != null) {
            maxPartitionsPerCanaryMember = Integer.parseInt(maxPartitionsValue.toString());
            if (maxPartitionsPerCanaryMember < 0) {
                throw new ConfigException(MAX_PARTITIONS_PER_MEMBER_CONFIG + " must be non-negative.");
            }
        } else {
            maxPartitionsPerCanaryMember = DEFAULT_MAX_PARTITIONS_PER_MEMBER;
        }

        LOG.info("Configured canary assignor with instanceIdPattern={}, maxPartitionsPerCanaryMember={}",
            instanceIdPattern, maxPartitionsPerCanaryMember);
    }

    @Override
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        // Get the subscribed topic ids from the first member (homogeneous).
        Set<Uuid> subscribedTopicIds = new HashSet<>(
            groupSpec.memberSubscription(groupSpec.memberIds().iterator().next()).subscribedTopicIds()
        );

        if (subscribedTopicIds.isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        // Classify members into canary and non-canary.
        List<String> canaryMembers = new ArrayList<>();
        List<String> nonCanaryMembers = new ArrayList<>();
        for (String memberId : groupSpec.memberIds()) {
            if (isCanaryMember(groupSpec, memberId)) {
                canaryMembers.add(memberId);
            } else {
                nonCanaryMembers.add(memberId);
            }
        }

        LOG.debug("Canary members: {}, non-canary members: {}", canaryMembers, nonCanaryMembers);

        // Compute the list of unassigned partitions and total partition count.
        List<TopicIdPartition> unassignedPartitions = new ArrayList<>();
        int totalPartitionsCount = 0;
        for (Uuid topicId : subscribedTopicIds) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
            if (partitionCount == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            }
            for (int i = 0; i < partitionCount; i++) {
                if (!groupSpec.isPartitionAssigned(topicId, i)) {
                    unassignedPartitions.add(new TopicIdPartition(topicId, i));
                }
            }
            totalPartitionsCount += partitionCount;
        }

        // Compute quotas.
        // Canary members collectively get min(canaryCount * max, total) partitions.
        int totalCanaryPartitions = Math.min(
            canaryMembers.size() * maxPartitionsPerCanaryMember,
            totalPartitionsCount
        );

        // Distribute totalCanaryPartitions uniformly among canary members.
        int canaryMinQuota = !canaryMembers.isEmpty()
            ? totalCanaryPartitions / canaryMembers.size() : 0;
        int canaryExtraMembers = !canaryMembers.isEmpty()
            ? totalCanaryPartitions % canaryMembers.size() : 0;

        // Non-canary members share the remaining partitions uniformly.
        int remainingPartitions = totalPartitionsCount - totalCanaryPartitions;
        int nonCanaryMinQuota = !nonCanaryMembers.isEmpty()
            ? remainingPartitions / nonCanaryMembers.size() : 0;
        int nonCanaryExtraMembers = !nonCanaryMembers.isEmpty()
            ? remainingPartitions % nonCanaryMembers.size() : 0;

        // Revoke excess partitions and build the target assignment.
        Map<String, MemberAssignment> targetAssignment = new HashMap<>();
        List<MemberWithRemainingQuota> unfilledMembers = new ArrayList<>();

        // Process canary members.
        int canaryRemainingExtra = canaryExtraMembers;
        for (String memberId : canaryMembers) {
            int quota = canaryMinQuota;
            if (canaryRemainingExtra > 0) {
                quota++;
                canaryRemainingExtra--;
            }

            processExistingAssignment(
                groupSpec,
                memberId,
                subscribedTopicIds,
                quota,
                targetAssignment,
                unfilledMembers,
                unassignedPartitions
            );
        }

        // Process non-canary members. The first nonCanaryExtraMembers members
        // receive one extra partition to ensure all partitions are assigned.
        int nonCanaryRemainingExtra = nonCanaryExtraMembers;
        for (String memberId : nonCanaryMembers) {
            int quota = nonCanaryMinQuota;
            if (nonCanaryRemainingExtra > 0) {
                quota++;
                nonCanaryRemainingExtra--;
            }

            processExistingAssignment(
                groupSpec,
                memberId,
                subscribedTopicIds,
                quota,
                targetAssignment,
                unfilledMembers,
                unassignedPartitions
            );
        }

        // Assign the remaining unassigned partitions to unfilled members.
        int unassignedPartitionIndex = 0;
        for (MemberWithRemainingQuota unfilledMember : unfilledMembers) {
            Map<Uuid, Set<Integer>> assignment = targetAssignment.get(unfilledMember.memberId).partitions();
            if (AssignorHelpers.isImmutableMap(assignment)) {
                assignment = AssignorHelpers.deepCopyAssignment(assignment);
                targetAssignment.put(unfilledMember.memberId, new MemberAssignmentImpl(assignment));
            }

            for (int i = 0; i < unfilledMember.remainingQuota && unassignedPartitionIndex < unassignedPartitions.size(); i++) {
                TopicIdPartition tp = unassignedPartitions.get(unassignedPartitionIndex);
                unassignedPartitionIndex++;
                assignment
                    .computeIfAbsent(tp.topicId(), __ -> new HashSet<>())
                    .add(tp.partitionId());
            }
        }

        if (unassignedPartitionIndex < unassignedPartitions.size()) {
            throw new PartitionAssignorException("Partitions were left unassigned.");
        }

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Determines whether a member is a canary member based on its instance id.
     */
    private boolean isCanaryMember(GroupSpec groupSpec, String memberId) {
        return groupSpec.memberSubscription(memberId).instanceId()
            .filter(id -> instanceIdPattern.matcher(id).matches())
            .isPresent();
    }

    /**
     * Processes the existing assignment for a member, revoking partitions that are
     * no longer subscribed or exceed the member's quota.
     */
    private void processExistingAssignment(
        GroupSpec groupSpec,
        String memberId,
        Set<Uuid> subscribedTopicIds,
        int quota,
        Map<String, MemberAssignment> targetAssignment,
        List<MemberWithRemainingQuota> unfilledMembers,
        List<TopicIdPartition> unassignedPartitions
    ) {
        Map<Uuid, Set<Integer>> oldAssignment = groupSpec.memberAssignment(memberId).partitions();
        Map<Uuid, Set<Integer>> newAssignment = null;

        if (!AssignorHelpers.isImmutableMap(oldAssignment)) {
            throw new IllegalStateException("The assignor expects an immutable map.");
        }

        int remainingQuota = quota;

        for (Map.Entry<Uuid, Set<Integer>> topicPartitions : oldAssignment.entrySet()) {
            Uuid topicId = topicPartitions.getKey();
            Set<Integer> partitions = topicPartitions.getValue();

            if (subscribedTopicIds.contains(topicId)) {
                if (partitions.size() <= remainingQuota) {
                    remainingQuota -= partitions.size();
                } else {
                    for (Integer partition : partitions) {
                        if (remainingQuota > 0) {
                            remainingQuota--;
                        } else {
                            if (newAssignment == null) {
                                newAssignment = AssignorHelpers.deepCopyAssignment(oldAssignment);
                            }
                            Set<Integer> parts = newAssignment.get(topicId);
                            parts.remove(partition);
                            if (parts.isEmpty()) {
                                newAssignment.remove(topicId);
                            }
                            unassignedPartitions.add(new TopicIdPartition(topicId, partition));
                        }
                    }
                }
            } else {
                if (newAssignment == null) {
                    newAssignment = AssignorHelpers.deepCopyAssignment(oldAssignment);
                }
                newAssignment.remove(topicId);
            }
        }

        if (remainingQuota > 0) {
            unfilledMembers.add(new MemberWithRemainingQuota(memberId, remainingQuota));
        }

        if (newAssignment == null) {
            targetAssignment.put(memberId, new MemberAssignmentImpl(oldAssignment));
        } else {
            targetAssignment.put(memberId, new MemberAssignmentImpl(newAssignment));
        }
    }

    private record MemberWithRemainingQuota(String memberId, int remainingQuota) {
    }
}
