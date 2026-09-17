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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.Uniform2Assignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.load;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the claims, fill and even out phases described in {@link AssignmentBuilder}.
 * Every expectation is worked out by hand in the comments.
 */
public class AllocationBuilderTest {
    // Topics and members are sorted by id, so these ids fix the indices: T1 is topic 0, T2 is
    // topic 1, and so on, while member "A" has index A, member "B" index B, and so on.
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(1L, 2L);
    private static final Uuid T3 = new Uuid(1L, 3L);
    private static final Uuid T4 = new Uuid(1L, 4L);
    private static final Uuid T5 = new Uuid(1L, 5L);
    private static final Uuid[] TOPICS = {T1, T2, T3, T4, T5};
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int D = 3;

    /**
     * @return A describer with T1, T2, T3, ... having the given numbers of partitions.
     */
    private static SubscribedTopicDescriber describer(int... partitionCounts) {
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        for (int i = 0; i < partitionCounts.length; i++) {
            builder.addTopic(TOPICS[i], "topic-" + (i + 1), partitionCounts[i], 3, 1);
        }
        return builder.buildDescriber();
    }

    /**
     * @return A describer with broker 0 in rack r1 and broker 1 in rack r2, and T1 with the given
     *         replicas per partition.
     */
    private static SubscribedTopicDescriber rackDescriber(List<List<Integer>> replicasPerPartition) {
        return new TestMetadataImageBuilder()
            .addBroker(0, "r1")
            .addBroker(1, "r2")
            .addTopic(T1, "topic-1", replicasPerPartition)
            .buildDescriber();
    }

    private static GroupModel model(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        boolean rackAwareEnabled
    ) {
        return new GroupModel(spec(members), describer, rackAwareEnabled);
    }

    /**
     * Runs the three phases and checks that every extra partition of every topic has a receiver.
     */
    private static Allocations assign(GroupModel model) {
        Allocations allocations = new AllocationBuilder(model).build();
        for (int t = 0; t < model.topicCount(); t++) {
            assertEquals(model.extraPartitionCount()[t], allocations.extraReceiverCount(t), "extra partitions of topic " + t + " with a receiver");
        }
        return allocations;
    }

    /**
     * Asserts that exactly the given members get an extra partition of the topic, and that the
     * allocations follow.
     */
    private static void assertExtraPartitions(
        GroupModel model,
        Allocations allocations,
        int topic,
        int... owners
    ) {
        Set<Integer> expected = new HashSet<>();
        for (int owner : owners) {
            expected.add(owner);
        }
        for (int m = 0; m < model.memberCount(); m++) {
            boolean has = expected.contains(m);
            assertEquals(has, allocations.hasExtra(m, topic), "member " + model.memberIds()[m] + " has an extra partition of topic " + topic);
            assertEquals(model.basePartitionCount()[topic] + (has ? 1 : 0), allocations.allocation(m, topic),
                "allocation of member " + model.memberIds()[m] + " for topic " + topic);
        }
    }

    /**
     * Asserts the load of every member: the sum of its allocations over its topics.
     */
    private static void assertLoads(GroupModel model, Allocations allocations, int... expectedLoads) {
        assertEquals(model.memberCount(), expectedLoads.length);
        for (int m = 0; m < model.memberCount(); m++) {
            int load = 0;
            for (int t : model.memberTopics()[m]) {
                load += allocations.allocation(m, t);
            }
            assertEquals(expectedLoads[m], load, "load of member " + model.memberIds()[m]);
        }
    }

    /**
     * First example of the class doc. A, B and C subscribe to T1 with 5 partitions and T2 with 4
     * partitions and own nothing: 1 base partition each of both topics, two extra partitions for
     * T1 and one for T2, a base load of 2 for everyone.
     * <pre>
     * Claims:   nothing is owned, so no claim.
     * Fill:     the extra partitions of T1 go to the two least loaded members; all loads are equal,
     *           so to A and B by id (load 3). The one of T2 goes to C, the least loaded by then.
     * Even out: all loads are 3, nothing to do.
     * </pre>
     */
    @Test
    public void testFreshGroupFillsTheLeastLoadedMembers() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2), Assignment.EMPTY));
        GroupModel model = model(members, describer(5, 4), false);

        Allocations allocations = assign(model);

        assertLoads(model, allocations, 3, 3, 3);
        // Ties between equally loaded members go to the first member in the load order of the
        // cohort: A receives the first extra partition of T1 and moves to the end of the order,
        // which becomes C, B, A, so C receives the second one and the order becomes B, C, A. The
        // extra partition of T2 then goes to B.
        assertExtraPartitions(model, allocations, 0, A, C);
        assertExtraPartitions(model, allocations, 1, B);
    }

    /**
     * A variant of the first example of the class doc: D joins A, B and C with the assignment of
     * the example. T1 now has 5 / 4 = 1 base partition and one extra partition, T2 has 4 / 4 = 1
     * base partition and none. Base load 2 for everyone.
     * <pre>
     * Claims:   A and B both own 2 partitions of T1, more than the base, and claim its only extra
     *           partition. Both are at load 2, so A wins by id (load 3). C holds 2 partitions of
     *           T2, but T2 has no extra partition to claim.
     * Fill:     every extra partition has a receiver.
     * Even out: A is at 3 and everyone else at 2, a gap of one: nothing moves.
     * </pre>
     */
    @Test
    public void testClaimsGoToTheLeastLoadedClaimantsThenById() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1), T2, Set.of(0)))));
        members.put("B", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(2, 3), T2, Set.of(1)))));
        members.put("C", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(4), T2, Set.of(2, 3)))));
        members.put("D", member(Set.of(T1, T2), Assignment.EMPTY));
        GroupModel model = model(members, describer(5, 4), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, A);
        assertExtraPartitions(model, allocations, 1);
        assertLoads(model, allocations, 3, 2, 2, 2);
        // The extra partition kept by A lets it keep one of its current partitions.
        assertTrue(model.isBacked(A, 0));
        assertEquals(0, allocations.freeExtraCount(A));
    }

    /**
     * The loads compared during the claims include the extra partitions claimed on earlier topics.
     * A, B and C subscribe to T1 and T2 with 4 partitions each: 1 base partition and one extra
     * partition per topic, base load 2.
     * <pre>
     * Claims:   T1: only A owns more than the base partition, it claims the extra partition (load 3).
     *           T2: A and B both hold more than the base partition, for a single extra partition.
     *               B at load 2 wins over A at load 3, although A comes first by id.
     * Fill:     every extra partition has a receiver.
     * Even out: A 3, B 3, C 2: nothing moves.
     * </pre>
     */
    @Test
    public void testClaimsCountTheExtraPartitionsOfEarlierTopics() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1)))));
        members.put("B", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(2), T2, Set.of(2, 3)))));
        members.put("C", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(3)))));
        GroupModel model = model(members, describer(4, 4), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, A);
        assertExtraPartitions(model, allocations, 1, B);
        assertLoads(model, allocations, 3, 3, 2);
    }

    /**
     * Second example of the class doc. A and B subscribe to T1 and T2 with 3 partitions each, and
     * A owns all six: 1 base partition and one extra partition per topic, base load 2.
     * <pre>
     * Claims:   A owns 3 partitions of T1 and claims its extra partition (load 3), then the one
     *           of T2 (load 4). B stays at 2.
     * Fill:     every extra partition has a receiver.
     * Even out: A is at 4 and B at 2, a gap of two, so A must give one. Both of its extra
     *           partitions are backed, so the first pass does nothing and the second pass moves
     *           the one of the lowest topic, T1, to B. Loads: A 3, B 3.
     * </pre>
     */
    @Test
    public void testEvenOutMovesAnExtraPartitionAcrossAGapOfTwo() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1, 2), T2, Set.of(0, 1, 2)))));
        members.put("B", member(Set.of(T1, T2), Assignment.EMPTY));
        GroupModel model = model(members, describer(3, 3), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, B);
        assertExtraPartitions(model, allocations, 1, A);
        assertLoads(model, allocations, 3, 3);
    }

    /**
     * Moving an extra partition across a gap of one would only swap who is heavier. A and B
     * subscribe to T1 with 3 partitions and T2 with 2: T1 has 1 base partition and one extra
     * partition, T2 has 1 base partition and none. A holds all of T1 and claims its extra partition
     * (load 3), B is at 2: nothing moves.
     */
    @Test
    public void testEvenOutIgnoresAGapOfOne() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1, 2), T2, Set.of(0)))));
        members.put("B", member(Set.of(T1, T2), new Assignment(Map.of(T2, Set.of(1)))));
        GroupModel model = model(members, describer(3, 2), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, A);
        assertExtraPartitions(model, allocations, 1);
        assertLoads(model, allocations, 3, 2);
    }

    /**
     * A member gives its free extra partitions before its backed ones. A subscribes to T1 and T2,
     * B to T1, T2 and T3, and C to T3. T1 and T2 have 3 partitions: 1 base partition and one extra
     * partition each for A and B. T3 has a single partition: no base partition and one extra
     * partition for B and C. Base loads: A 2, B 2, C 0.
     * <pre>
     * Claims:   A holds all of T1 and claims its extra partition (load 3). B holds the partition
     *           of T3 and claims its extra partition (load 3).
     * Fill:     the extra partition of T2 goes to the least loaded subscriber. A and B are tied at
     *           3 and A, in the first cohort, wins. It is free for A, which holds nothing of T2.
     *           Loads: A 4, B 3, C 0.
     * Even out: A cannot give anything to B, one below it. B, at 3, is three above C and gives it
     *           the extra partition of T3 (B 2, C 1). A, at 4, is now two above B, a subscriber
     *           of both T1 and T2 with neither extra partition: A gives the free extra partition
     *           of T2 rather than the backed one of T1, so that it keeps its current partitions.
     *           Loads: A 3, B 3, C 1.
     * </pre>
     */
    @Test
    public void testEvenOutMovesFreeExtraPartitionsBeforeBackedOnes() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1, 2)))));
        members.put("B", member(Set.of(T1, T2, T3), new Assignment(Map.of(T3, Set.of(0)))));
        members.put("C", member(Set.of(T3), Assignment.EMPTY));
        GroupModel model = model(members, describer(3, 3, 1), false);

        Allocations allocations = assign(model);

        assertTrue(model.isBacked(A, 0));
        assertFalse(model.isBacked(A, 1));
        assertExtraPartitions(model, allocations, 0, A);
        assertExtraPartitions(model, allocations, 1, B);
        assertExtraPartitions(model, allocations, 2, C);
        assertLoads(model, allocations, 3, 3, 1);
    }

    /**
     * The most loaded member gives first, so that no member gives up an extra partition it would
     * need back later. A, B and C subscribe to T1 to T5 with 4 partitions each: 1 base partition
     * and one extra partition per topic, base load 5.
     * <pre>
     * Claims:   A holds all of T1, T2 and T3 and claims their extra partitions (load 8). B holds
     *           all of T4 and T5 and claims theirs (load 7). C holds nothing (load 5).
     * Fill:     every extra partition has a receiver.
     * Even out: all extra partitions are backed, so only the second pass acts. A, the most loaded,
     *           is three above C and gives it one extra partition: the one of T1, the lowest topic
     *           since C is the receiver for all of them. Loads: A 7, B 7, C 6. Nothing else moves.
     * </pre>
     * Had B given first, it would have moved the extra partition of T4 to C, and A would then have
     * moved one of its own to B or C: two moves instead of one.
     */
    @Test
    public void testEvenOutTakesFromTheMostLoadedMemberFirst() {
        Set<Uuid> topics = Set.of(T1, T2, T3, T4, T5);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(topics, new Assignment(Map.of(
            T1, Set.of(0, 1, 2, 3), T2, Set.of(0, 1, 2, 3), T3, Set.of(0, 1, 2, 3)))));
        members.put("B", member(topics, new Assignment(Map.of(T4, Set.of(0, 1, 2, 3), T5, Set.of(0, 1, 2, 3)))));
        members.put("C", member(topics, Assignment.EMPTY));
        GroupModel model = model(members, describer(4, 4, 4, 4, 4), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, C);
        assertExtraPartitions(model, allocations, 1, A);
        assertExtraPartitions(model, allocations, 2, A);
        assertExtraPartitions(model, allocations, 3, B);
        assertExtraPartitions(model, allocations, 4, B);
        assertLoads(model, allocations, 7, 7, 6);
    }

    /**
     * With different subscriptions, the even out phase stops when no single move can bring two
     * loads closer by two, even if a chain of moves could. A subscribes to T1 and T3, B to T1 and
     * T2, C to T2 and T4. T1 and T2 have 3 partitions shared by two members: 1 base partition and
     * one extra partition each. T3 has 2 partitions for A alone and T4 1 partition for C alone: no
     * extra partition. Base loads: A 3, B 2, C 2.
     * <pre>
     * Claims:   A holds all of T1 and claims its extra partition (load 4). B holds all of T2 and
     *           claims its extra partition (load 3).
     * Fill:     every extra partition has a receiver.
     * Even out: A is two above C, but C is not subscribed to T1, and B, the only other subscriber
     *           of T1, is one below A. B is one above C. No single move helps: the phase ends at
     *           A 4, B 3, C 2, although moving T1 to B and then T2 to C would reach 3, 3, 3.
     * </pre>
     */
    @Test
    public void testEvenOutStopsWhenNoSingleMoveHelps() {
        SubscribedTopicDescriber describer = describer(3, 3, 2, 1);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T3), new Assignment(Map.of(T1, Set.of(0, 1, 2)))));
        members.put("B", member(Set.of(T1, T2), new Assignment(Map.of(T2, Set.of(0, 1, 2)))));
        members.put("C", member(Set.of(T2, T4), Assignment.EMPTY));
        GroupModel model = model(members, describer, false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, A);
        assertExtraPartitions(model, allocations, 1, B);
        assertExtraPartitions(model, allocations, 2);
        assertExtraPartitions(model, allocations, 3);
        assertLoads(model, allocations, 4, 3, 2);

        // The full assignor reaches the same loads, and the assignment has every property.
        GroupAssignment result = new Uniform2Assignor(false).assign(spec(members), describer);
        assertValidAssignment(members, describer, result);
        assertEquals(4, load(result, "A"));
        assertEquals(3, load(result, "B"));
        assertEquals(2, load(result, "C"));
    }

    /**
     * When rack aware, the fill breaks ties between equally loaded members in favor of the one
     * whose rack has the most spare replicas of the topic. A in rack r1 and B in rack r2 subscribe
     * to T1 with 3 partitions: 1 base partition each and one extra partition, equal loads. Both
     * racks have a replica for the base partition of their member; only the rack with two replicas
     * has one left for the extra partition, so its member gets it, whatever its id.
     */
    @Test
    public void testFillBreaksTiesByRackSpareReplicas() {
        // Brokers 0 and 1 are in r1 and r2. Two partitions have their replica in r1 and one in r2.
        SubscribedTopicDescriber mostlyInR1 = rackDescriber(List.of(List.of(0), List.of(0), List.of(1)));
        // Two partitions have their replica in r2 and one in r1.
        SubscribedTopicDescriber mostlyInR2 = rackDescriber(List.of(List.of(1), List.of(1), List.of(0)));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1), Assignment.EMPTY));
        members.put("B", member("r2", Set.of(T1), Assignment.EMPTY));

        GroupModel model = model(members, mostlyInR1, true);
        assertExtraPartitions(model, assign(model), 0, A);

        model = model(members, mostlyInR2, true);
        assertExtraPartitions(model, assign(model), 0, B);

        // Without rack awareness, the tie goes to A whatever the replicas.
        model = model(members, mostlyInR2, false);
        assertExtraPartitions(model, assign(model), 0, A);
    }

    /**
     * A topic without extra partitions leaves every allocation at the base. A and B subscribe to T1
     * with 4 partitions (2 base partitions, no extra partition), T2 without partitions and T3 with
     * 3 partitions (1 base partition and one extra partition, which goes to A by id).
     */
    @Test
    public void testTopicWithoutExtraPartitionsKeepsTheBaseAllocations() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        members.put("B", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        GroupModel model = model(members, describer(4, 0, 3), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0);
        assertEquals(2, allocations.allocation(A, 0));
        assertEquals(2, allocations.allocation(B, 0));
        assertExtraPartitions(model, allocations, 1);
        assertEquals(0, allocations.allocation(A, 1));
        assertEquals(0, allocations.allocation(B, 1));
        assertExtraPartitions(model, allocations, 2, A);
        assertEquals(2, allocations.allocation(A, 2));
        assertEquals(1, allocations.allocation(B, 2));
        assertLoads(model, allocations, 4, 3);
    }

    /**
     * A, B, C and D subscribe to T1, T2 and T3 with 5 partitions each: base 1 and one extra
     * partition per topic, base load 3. A holds T1 {0, 1}, T2 {0..4} and T3 {0..4}, D holds
     * T1 {2, 3}.
     * <pre>
     * Claims:   T1: A and D both hold more than the base, tie at load 3, A wins by id (load 4).
     *           T2 and T3: A claims (loads 5 and 6). Loads: A 6, B 3, C 3, D 3.
     * Even out: A must give an extra partition. All of its extra partitions are backed, so the
     *           second pass moves the one of the lowest topic, T1. The least loaded candidates
     *           are B, C and D; D holds two partitions of T1, so D wins and keeps one of them
     *           (load 4). A (5) then gives T2 to B, the first of B and C in the load order.
     * </pre>
     * Only one partition of T1 moves, where giving T1 to B would have moved two.
     */
    @Test
    public void testEvenOutPrefersReceiversHoldingPartitionsOfTheTopic() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2, T3), new Assignment(Map.of(
            T1, Set.of(0, 1), T2, Set.of(0, 1, 2, 3, 4), T3, Set.of(0, 1, 2, 3, 4)))));
        members.put("B", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        members.put("D", member(Set.of(T1, T2, T3), new Assignment(Map.of(T1, Set.of(2, 3)))));
        GroupModel model = model(members, describer(5, 5, 5), false);

        Allocations allocations = assign(model);

        assertExtraPartitions(model, allocations, 0, D);
        assertExtraPartitions(model, allocations, 1, B);
        assertExtraPartitions(model, allocations, 2, A);
        assertLoads(model, allocations, 4, 4, 3, 4);
    }
}
