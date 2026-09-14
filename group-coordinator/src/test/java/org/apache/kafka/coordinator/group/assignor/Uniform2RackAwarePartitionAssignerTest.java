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
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.alignedPartitions;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.assertStable;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.revocations;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the partition phase with racks, see {@link Uniform2RackAwarePartitionAssigner}: keep,
 * align, leftovers and swap. Brokers 0, 1 and 2 are in rack-0, rack-1 and rack-2, so with two
 * replicas partition {@code i} has replicas in rack-(i % 3) and rack-((i + 1) % 3). Only the racks
 * of the members count: a replica in a rack without member does not align anything. Every
 * expectation is worked out by hand in the comments.
 */
public class Uniform2RackAwarePartitionAssignerTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final String A = "A";
    private static final String B = "B";
    private static final String C = "C";
    private static final String RACK_0 = "rack-0";
    private static final String RACK_1 = "rack-1";
    private static final String RACK_2 = "rack-2";
    private static final Set<Uuid> TOPICS = Set.of(T1);

    private final Uniform2Assignor assignor = new Uniform2Assignor(true);

    /**
     * @return T1 with the given number of partitions and two replicas: partition i has replicas in
     *         rack-(i % 3) and rack-((i + 1) % 3).
     */
    private static SubscribedTopicDescriber describer(int partitions) {
        return new TestMetadataImageBuilder()
            .addBroker(0, RACK_0).addBroker(1, RACK_1).addBroker(2, RACK_2)
            .addTopic(T1, "topic-1", partitions, 3, 2)
            .buildDescriber();
    }

    /**
     * @return T1 with the given replicas per partition. Broker 3 is in rack-3, where no member is.
     */
    private static SubscribedTopicDescriber describer(List<List<Integer>> replicasPerPartition) {
        return new TestMetadataImageBuilder()
            .addBroker(0, RACK_0).addBroker(1, RACK_1).addBroker(2, RACK_2).addBroker(3, "rack-3")
            .addTopic(T1, "topic-1", replicasPerPartition)
            .buildDescriber();
    }

    /**
     * Runs the extra partition phases, then the partition phase with racks.
     */
    private static GroupAssignment assign(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer
    ) {
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members), describer, true);
        assertTrue(model.usesRacks);
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitionAssigner(model).assign();
        return new Uniform2RackAwarePartitionAssigner(model, extras).assign();
    }

    private static Assignment holding(Integer... partitions) {
        return new Assignment(Map.of(T1, Set.of(partitions)));
    }

    private static Set<Integer> partitions(GroupAssignment result, String memberId) {
        return result.members().get(memberId).partitions().get(T1);
    }

    @Test
    public void testFreshGroupIsFullyAligned() {
        // 6 partitions for 3 members in 3 racks: a quota of 2 each. Every rack has replicas of 4
        // partitions, so every member can get 2 partitions with a replica in its rack.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, Assignment.EMPTY));
        members.put(B, member(RACK_1, TOPICS, Assignment.EMPTY));
        members.put(C, member(RACK_2, TOPICS, Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        for (String memberId : members.keySet()) {
            assertEquals(2, partitions(result, memberId).size(), memberId);
        }
        assertEquals(6, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testMisalignedPartitionIsReleasedAndRealignedToAnotherMember() {
        // 2 partitions for A in rack-0 and B in rack-1: a quota of 1 each. Partition 0 has replicas
        // in rack-0 and rack-1, partition 1 in rack-1 and rack-2. A holds 1, misaligned, and
        // releases it. Partition 1 can only be aligned with B and partition 0 with A.
        SubscribedTopicDescriber describer = describer(2);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(1)));
        members.put(B, member(RACK_1, TOPICS, Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(0), partitions(result, A));
        assertEquals(Set.of(1), partitions(result, B));
        assertEquals(2, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testMisalignedPartitionsComeBackToTheirHolderWhenTheyCannotBeAligned() {
        // A in rack-0 and B in rack-1, a quota of 2 each. Replica racks, counting only member
        // racks: partition 0 in rack-0 and rack-1, partition 1 in rack-1, partition 2 in rack-0,
        // partition 3 in none. A holds 1 and 3, both misaligned, and releases both. B keeps 0 and
        // releases 2, misaligned for it. The flow aligns 2 with A and 1 with B; 3 cannot be
        // aligned with anyone and comes back to A, its previous holder, which is still below
        // its quota. No swap can help since 3 has no replica in a member rack.
        SubscribedTopicDescriber describer = describer(List.of(List.of(0, 1), List.of(1, 2), List.of(0, 2), List.of(3)));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(1, 3)));
        members.put(B, member(RACK_1, TOPICS, holding(0, 2)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(2, 3), partitions(result, A));
        assertEquals(Set.of(0, 1), partitions(result, B));
        assertEquals(3, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testHolderAboveQuotaReleasesThePartitionsMostUsefulToRacksInDeficit() {
        // 6 partitions for A in rack-0 and B in rack-1: a quota of 3 each. Replica racks, counting
        // only member racks: 0 and 3 in both racks, 1 and 4 in rack-1, 2 and 5 in rack-0. A holds
        // 0, 2, 3 and 5, all aligned, one too many. B holds 1 and 4, aligned, and needs one more,
        // so rack-1 has a deficit of 1. A releases the partition most useful to rack-1: 0 and 3
        // have a replica there and 2 and 5 do not, so A keeps 2 and 5, then the lowest of 0 and
        // 3, and releases 3, which goes to B. Keeping the lowest ids would have released 5,
        // which B cannot use.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(0, 2, 3, 5)));
        members.put(B, member(RACK_1, TOPICS, holding(1, 4)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(0, 2, 5), partitions(result, A));
        assertEquals(Set.of(1, 3, 4), partitions(result, B));
        assertEquals(6, alignedPartitions(members, result, describer));
        assertEquals(1, revocations(members, result));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testAlignmentIsFoundByMaximumFlowWhereGreedyFails() {
        // 3 partitions for 3 members in 3 racks: a quota of 1 each. Partition 0 has replicas in
        // rack-1 and rack-2, partitions 1 and 2 in rack-0 and rack-1. Handing partitions out in
        // order to the first rack with a replica and a deficit would give 0 to rack-1, 1 to
        // rack-0 and leave 2 without an aligned member. Full alignment requires 0 to go to
        // rack-2, which the flow finds: within a group, partitions are handed to the racks in
        // rack order, so A gets 1 and B gets 2.
        SubscribedTopicDescriber describer = describer(List.of(List.of(1, 2), List.of(0, 1), List.of(0, 1)));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, Assignment.EMPTY));
        members.put(B, member(RACK_1, TOPICS, Assignment.EMPTY));
        members.put(C, member(RACK_2, TOPICS, Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(1), partitions(result, A));
        assertEquals(Set.of(2), partitions(result, B));
        assertEquals(Set.of(0), partitions(result, C));
        assertEquals(3, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testMisalignedPartitionIsSwappedWithAnAlignedOne() {
        // 4 partitions for A in rack-0 and B in rack-1: a quota of 2 each. Replica racks, counting
        // only member racks: 0 and 3 in both racks, 1 in rack-1, 2 in rack-0. A holds 1 and 2, B
        // holds 0 and 3. A releases 1, misaligned; nobody in rack-1 is below its quota, so 1
        // comes back to A. It is then swapped with a partition of B, which is in a replica rack
        // of 1, having a replica in rack-0: 0, the first of 0 and 3. Both keep their quota.
        SubscribedTopicDescriber describer = describer(4);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(1, 2)));
        members.put(B, member(RACK_1, TOPICS, holding(0, 3)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(0, 2), partitions(result, A));
        assertEquals(Set.of(1, 3), partitions(result, B));
        assertEquals(4, alignedPartitions(members, result, describer));
        assertEquals(2, revocations(members, result));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testSettledButMisalignedTopicIsRealigned() {
        // 6 partitions for 3 members in 3 racks holding 2 each: the quotas are met, but every
        // partition is held by the member of the one rack without a replica of it. Everything
        // is released and realigned, so every partition moves.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(1, 4)));
        members.put(B, member(RACK_1, TOPICS, holding(2, 5)));
        members.put(C, member(RACK_2, TOPICS, holding(0, 3)));
        assertEquals(0, alignedPartitions(members, currentAssignment(members), describer));

        GroupAssignment result = assign(members, describer);

        for (String memberId : members.keySet()) {
            assertEquals(2, partitions(result, memberId).size(), memberId);
        }
        assertEquals(6, alignedPartitions(members, result, describer));
        assertEquals(6, revocations(members, result));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testSettledAndAlignedTopicIsEmittedAsIs() {
        // 6 partitions for 3 members in 3 racks holding 2 aligned partitions each: 0 and 3 have a
        // replica in rack-0, 1 and 4 in rack-1, 2 and 5 in rack-2. Nothing moves and every
        // member gets its own map and set back.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, holding(0, 3)));
        members.put(B, member(RACK_1, TOPICS, holding(1, 4)));
        members.put(C, member(RACK_2, TOPICS, holding(2, 5)));

        GroupAssignment result = assign(members, describer);

        for (String memberId : members.keySet()) {
            Map<Uuid, Set<Integer>> current = members.get(memberId).partitions();
            assertSame(current, result.members().get(memberId).partitions());
            assertSame(current.get(T1), partitions(result, memberId));
        }
        assertEquals(6, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testPartitionWithoutReplicaInAnyMemberRackIsStillAssigned() {
        // 3 partitions for 3 members in 3 racks: a quota of 1 each. Partition 0 has replicas in
        // rack-0 and rack-1, partition 1 in rack-1 and rack-2, partition 2 only in rack-3, where
        // no member is. The flow aligns 0 with A and 1 with B; 2 is a leftover without previous
        // holder and goes to the remaining member below its quota, C. Feeding the result back
        // changes nothing: 2 is released by C as misaligned and comes back to it.
        SubscribedTopicDescriber describer = describer(List.of(List.of(0, 1), List.of(1, 2), List.of(3)));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(RACK_0, TOPICS, Assignment.EMPTY));
        members.put(B, member(RACK_1, TOPICS, Assignment.EMPTY));
        members.put(C, member(RACK_2, TOPICS, Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(0), partitions(result, A));
        assertEquals(Set.of(1), partitions(result, B));
        assertEquals(Set.of(2), partitions(result, C));
        assertEquals(2, alignedPartitions(members, result, describer));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testRacksAreIgnoredWhenRackAwarenessIsDisabled() {
        // The same group with racks and rack awareness disabled, and without racks, give the
        // very same result: A keeps its lowest partitions and the rest goes to B then C, racks
        // notwithstanding.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> racked = new TreeMap<>();
        racked.put(A, member(RACK_0, TOPICS, holding(0, 1, 2, 3, 4, 5)));
        racked.put(B, member(RACK_1, TOPICS, Assignment.EMPTY));
        racked.put(C, member(RACK_2, TOPICS, Assignment.EMPTY));
        Map<String, MemberSubscriptionAndAssignmentImpl> unracked = new TreeMap<>();
        racked.forEach((memberId, m) -> unracked.put(memberId, member(m.subscribedTopicIds(), new Assignment(m.partitions()))));

        Uniform2GroupModel model = new Uniform2GroupModel(spec(racked), describer, false);
        assertFalse(model.usesRacks);
        GroupAssignment withRacks = new Uniform2PartitionAssigner(model, new Uniform2ExtraPartitionAssigner(model).assign()).assign();
        Uniform2GroupModel unrackedModel = new Uniform2GroupModel(spec(unracked), describer, false);
        GroupAssignment withoutRacks = new Uniform2PartitionAssigner(unrackedModel, new Uniform2ExtraPartitionAssigner(unrackedModel).assign()).assign();

        assertEquals(withoutRacks, withRacks);
        assertEquals(Set.of(0, 1), partitions(withRacks, A));
        assertEquals(Set.of(2, 3), partitions(withRacks, B));
        assertEquals(Set.of(4, 5), partitions(withRacks, C));
        // Partition 1 has no replica in rack-0 and partition 2 none in rack-1: 4 of 6 are aligned,
        // where the rack aware phase would align all 6.
        assertEquals(4, alignedPartitions(racked, withRacks, describer));
        assertValidAssignment(racked, describer, withRacks);
    }

    /**
     * @return The current assignment of the members as a group assignment, to measure its alignment.
     */
    private static GroupAssignment currentAssignment(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        Map<String, MemberAssignment> current = new TreeMap<>();
        members.forEach((memberId, m) -> current.put(memberId, new MemberAssignmentImpl(m.partitions())));
        return new GroupAssignment(current);
    }
}
