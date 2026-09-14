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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared helpers for the uniform2 tests: building groups, and checking the properties that every
 * assignment must have, see {@link Uniform2AssignmentBuilder}.
 */
final class Uniform2TestUtils {
    private Uniform2TestUtils() { }

    /**
     * @return A member without rack.
     */
    static MemberSubscriptionAndAssignmentImpl member(Set<Uuid> topics, Assignment assignment) {
        return member(null, topics, assignment);
    }

    /**
     * @return A member with the given rack, or without one when it is null.
     */
    static MemberSubscriptionAndAssignmentImpl member(String rack, Set<Uuid> topics, Assignment assignment) {
        return new MemberSubscriptionAndAssignmentImpl(Optional.ofNullable(rack), Optional.empty(), topics, assignment);
    }

    /**
     * @return A group spec for the members, with the subscription type derived from their
     *         subscriptions and the inverted target assignment derived from their partitions.
     */
    static GroupSpec spec(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        return new GroupSpecImpl(members, subscriptionType(members), invertedTargetAssignment(members));
    }

    /**
     * @return HOMOGENEOUS when every member has the same subscription, HETEROGENEOUS otherwise.
     */
    static SubscriptionType subscriptionType(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        Set<Set<Uuid>> subscriptions = new HashSet<>();
        members.values().forEach(m -> subscriptions.add(new HashSet<>(m.subscribedTopicIds())));
        return subscriptions.size() <= 1 ? SubscriptionType.HOMOGENEOUS : SubscriptionType.HETEROGENEOUS;
    }

    /**
     * @return Per topic and partition, the member currently holding it.
     */
    static Map<Uuid, Map<Integer, String>> invertedTargetAssignment(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        Map<Uuid, Map<Integer, String>> inverted = new HashMap<>();
        members.forEach((memberId, member) -> member.partitions().forEach((topicId, partitions) -> {
            Map<Integer, String> holders = inverted.computeIfAbsent(topicId, k -> new HashMap<>());
            partitions.forEach(partition -> holders.put(partition, memberId));
        }));
        return inverted;
    }

    /**
     * @return The members with the given assignment as their current one, in member id order.
     */
    static Map<String, MemberSubscriptionAndAssignmentImpl> withAssignment(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        GroupAssignment assignment
    ) {
        Map<String, MemberSubscriptionAndAssignmentImpl> result = new TreeMap<>();
        members.forEach((memberId, member) -> {
            MemberAssignment memberAssignment = assignment.members().get(memberId);
            result.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                member.rackId(),
                member.instanceId(),
                member.subscribedTopicIds(),
                memberAssignment == null ? Assignment.EMPTY : new Assignment(memberAssignment.partitions())
            ));
        });
        return result;
    }

    /**
     * Checks the properties of an assignment: every member of the group is in it, every
     * partition of every subscribed topic is assigned exactly once to a subscriber, every topic
     * is spread, so that its subscribers get its base partitions or one more, no extra partition
     * could move between two subscribers so that their loads get closer by two, and with a
     * single subscription all loads are within one of each other.
     */
    static void assertValidAssignment(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        GroupAssignment result
    ) {
        assertValidAssignment(members, describer, result, "");
    }

    static void assertValidAssignment(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        GroupAssignment result,
        String context
    ) {
        assertEquals(members.keySet(), result.members().keySet(), context);

        Set<Uuid> allTopics = new HashSet<>();
        members.values().forEach(m -> allTopics.addAll(m.subscribedTopicIds()));

        Map<Uuid, Map<Integer, String>> holders = new HashMap<>();
        Map<String, Integer> loads = new HashMap<>();
        for (Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            String id = entry.getKey();
            int load = 0;
            for (Map.Entry<Uuid, Set<Integer>> topicEntry : entry.getValue().partitions().entrySet()) {
                Uuid topicId = topicEntry.getKey();
                assertTrue(members.get(id).subscribedTopicIds().contains(topicId),
                    context + ": " + id + " is not subscribed to " + topicId);
                assertFalse(topicEntry.getValue().isEmpty(), context + ": empty partition set for " + id + " and " + topicId);
                int numPartitions = describer.numPartitions(topicId);
                for (int partition : topicEntry.getValue()) {
                    assertTrue(partition >= 0 && partition < numPartitions,
                        context + ": " + topicId + "-" + partition + " does not exist");
                    assertNull(holders.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition, id),
                        context + ": " + topicId + "-" + partition + " is assigned twice");
                    load++;
                }
            }
            loads.put(id, load);
        }
        for (Uuid topicId : allTopics) {
            int numPartitions = describer.numPartitions(topicId);
            assertEquals(numPartitions, holders.getOrDefault(topicId, Map.of()).size(),
                context + ": topic " + topicId + " is not fully assigned");
        }

        for (Uuid topicId : allTopics) {
            List<String> subscribers = new ArrayList<>();
            members.forEach((id, m) -> {
                if (m.subscribedTopicIds().contains(topicId)) {
                    subscribers.add(id);
                }
            });
            int base = describer.numPartitions(topicId) / subscribers.size();
            List<String> withExtra = new ArrayList<>();
            List<String> withoutExtra = new ArrayList<>();
            for (String id : subscribers) {
                int count = result.members().get(id).partitions().getOrDefault(topicId, Set.of()).size();
                assertTrue(count == base || count == base + 1,
                    context + ": " + id + " has " + count + " partitions of " + topicId + " with a base of " + base);
                (count == base + 1 ? withExtra : withoutExtra).add(id);
            }
            for (String giver : withExtra) {
                for (String receiver : withoutExtra) {
                    assertTrue(loads.get(giver) < loads.get(receiver) + 2,
                        context + ": an extra partition of " + topicId + " could move from " + giver + " at load "
                            + loads.get(giver) + " to " + receiver + " at load " + loads.get(receiver));
                }
            }
        }

        if (subscriptionType(members) == SubscriptionType.HOMOGENEOUS && !loads.isEmpty()) {
            int min = Collections.min(loads.values());
            int max = Collections.max(loads.values());
            assertTrue(max - min <= 1, context + ": loads are not within one of each other: " + loads);
        }
    }

    /**
     * Checks that assigning the result again returns the very same partition sets, which the
     * coordinator relies on to recognize unchanged members.
     */
    static void assertStable(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        GroupAssignment result,
        Uniform2Assignor assignor
    ) {
        Map<String, MemberSubscriptionAndAssignmentImpl> stableMembers = withAssignment(members, result);
        GroupAssignment again = assignor.assign(spec(stableMembers), describer);
        for (String id : stableMembers.keySet()) {
            assertSame(stableMembers.get(id).partitions(), again.members().get(id).partitions(),
                "the assignment of " + id + " is not a fixed point");
        }
    }

    /**
     * @return The number of partitions of the assignment having a replica in the rack of their member.
     */
    static int alignedPartitions(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        GroupAssignment assignment,
        SubscribedTopicDescriber describer
    ) {
        int aligned = 0;
        for (Map.Entry<String, MemberAssignment> entry : assignment.members().entrySet()) {
            String rack = members.get(entry.getKey()).rackId().orElse(null);
            for (Map.Entry<Uuid, Set<Integer>> topicEntry : entry.getValue().partitions().entrySet()) {
                for (int partition : topicEntry.getValue()) {
                    if (describer.racksForPartition(topicEntry.getKey(), partition).contains(rack)) {
                        aligned++;
                    }
                }
            }
        }
        return aligned;
    }

    /**
     * @return The number of current partitions of the members that they do not have in the assignment.
     */
    static int revocations(Map<String, MemberSubscriptionAndAssignmentImpl> members, GroupAssignment assignment) {
        int revocations = 0;
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : members.entrySet()) {
            MemberAssignment memberAssignment = assignment.members().get(entry.getKey());
            Map<Uuid, Set<Integer>> newPartitions = memberAssignment == null ? Map.of() : memberAssignment.partitions();
            for (Map.Entry<Uuid, Set<Integer>> topicEntry : entry.getValue().partitions().entrySet()) {
                Set<Integer> kept = newPartitions.getOrDefault(topicEntry.getKey(), Set.of());
                for (int partition : topicEntry.getValue()) {
                    if (!kept.contains(partition)) {
                        revocations++;
                    }
                }
            }
        }
        return revocations;
    }

    /**
     * @return The total number of partitions assigned to the member.
     */
    static int load(GroupAssignment assignment, String memberId) {
        MemberAssignment memberAssignment = assignment.members().get(memberId);
        if (memberAssignment == null) {
            return 0;
        }
        return memberAssignment.partitions().values().stream().mapToInt(Set::size).sum();
    }
}
