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
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.Uniform2Assignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertStable;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the partition phase without racks, see {@link PartitionAssigner}: which
 * partition ids every member gets once the allocations are known. The allocations come from
 * {@link AllocationBuilder}, and every expectation is worked out by hand in the
 * comments.
 */
public class PartitionAssignerTest {
    // Topics and members are sorted by id, so T1 is processed before T2 and A before B, and so on.
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(1L, 2L);
    private static final String A = "A";
    private static final String B = "B";
    private static final String C = "C";
    private static final String D = "D";
    private static final String E = "E";

    private final Uniform2Assignor assignor = new Uniform2Assignor();

    /**
     * @return A describer with T1, then T2, having the given numbers of partitions. A count of
     *         zero adds the topic without partitions.
     */
    private static SubscribedTopicDescriber describer(int... partitionCounts) {
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        builder.addTopic(T1, "topic-1", partitionCounts[0], 3, 1);
        if (partitionCounts.length > 1) {
            builder.addTopic(T2, "topic-2", partitionCounts[1], 3, 1);
        }
        return builder.buildDescriber();
    }

    /**
     * Runs the extra partition phases, then the partition phase without racks.
     */
    private static GroupAssignment assign(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer
    ) {
        GroupModel model = new GroupModel(spec(members), describer, false);
        assertFalse(model.usesRacks());
        Allocations allocations = new AllocationBuilder(model).build();
        return new PartitionAssigner(model, allocations).assign();
    }

    private static Assignment holding(Uuid topicId, Integer... partitions) {
        return new Assignment(Map.of(topicId, Set.of(partitions)));
    }

    private static Map<Uuid, Set<Integer>> partitions(GroupAssignment result, String memberId) {
        return result.members().get(memberId).partitions();
    }

    @Test
    public void testOwnersKeepTheirLowestPartitionsUpToTheirAllocationAndReleaseTheRest() {
        // T1 has 6 partitions for 3 members: a allocation of 2 each. A holds 4 and keeps the two
        // lowest, 0 and 2, releasing 4 and 5 to C. B owns exactly its allocation.
        SubscribedTopicDescriber describer = describer(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), holding(T1, 0, 2, 4, 5)));
        members.put(B, member(Set.of(T1), holding(T1, 1, 3)));
        members.put(C, member(Set.of(T1), Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(0, 2)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(1, 3)), partitions(result, B));
        assertEquals(Map.of(T1, Set.of(4, 5)), partitions(result, C));
        // B did not change and gets its own map back.
        assertSame(members.get(B).partitions(), partitions(result, B));
        assertValidAssignment(members, describer, result);
    }

    @Test
    public void testReleasedPartitionsGoToOwnersFirstThenToOtherSubscribersInMemberOrder() {
        // T1 has 8 partitions for 4 members: a allocation of 2 each. B holds 3 and releases 2, D holds
        // 1, A and C own nothing. Partitions 2, 4, 5, 6 and 7 are handed out in ascending order
        // to the members below their allocation, the owners first: D gets 2 before A, which comes
        // first by id but holds nothing, then A gets 4 and 5 and C gets 6 and 7.
        SubscribedTopicDescriber describer = describer(8);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), Assignment.EMPTY));
        members.put(B, member(Set.of(T1), holding(T1, 0, 1, 2)));
        members.put(C, member(Set.of(T1), Assignment.EMPTY));
        members.put(D, member(Set.of(T1), holding(T1, 3)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(4, 5)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(0, 1)), partitions(result, B));
        assertEquals(Map.of(T1, Set.of(6, 7)), partitions(result, C));
        assertEquals(Map.of(T1, Set.of(2, 3)), partitions(result, D));
        assertValidAssignment(members, describer, result);
    }

    @Test
    public void testBaseZeroOnlyGivesPartitionsToMembersWithAnExtraPartition() {
        // T1 has 3 partitions for 5 members: a base of 0 and three extra partitions. B and D hold
        // one partition each, more than the base, and claim an extra partition. The third one
        // goes to the least loaded subscriber without one: A, C and E are all at 0 and the load
        // order starts in id order, so A. Only A, B and D receive partitions, and A gets the
        // only unheld one.
        SubscribedTopicDescriber describer = describer(3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), Assignment.EMPTY));
        members.put(B, member(Set.of(T1), holding(T1, 1)));
        members.put(C, member(Set.of(T1), Assignment.EMPTY));
        members.put(D, member(Set.of(T1), holding(T1, 0)));
        members.put(E, member(Set.of(T1), Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(2)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(1)), partitions(result, B));
        assertEquals(Map.of(), partitions(result, C));
        assertEquals(Map.of(T1, Set.of(0)), partitions(result, D));
        assertEquals(Map.of(), partitions(result, E));
        // B and D keep what they hold, C and E own nothing: all four get their own map back.
        for (String memberId : Set.of(B, C, D, E)) {
            assertSame(members.get(memberId).partitions(), partitions(result, memberId));
        }
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testSettledTopicIsEmittedAsIs() {
        // T1 has 4 partitions for 2 members holding 2 each: every owner has exactly its allocation
        // and every partition is held, so nothing moves and the input comes back as is.
        SubscribedTopicDescriber describer = describer(4);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), holding(T1, 0, 1)));
        members.put(B, member(Set.of(T1), holding(T1, 2, 3)));

        GroupAssignment result = assign(members, describer);

        for (String memberId : members.keySet()) {
            Map<Uuid, Set<Integer>> current = members.get(memberId).partitions();
            assertSame(current, partitions(result, memberId));
            assertSame(current.get(T1), partitions(result, memberId).get(T1));
        }
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testCurrentPartitionsBeyondThePartitionCountAreStale() {
        // T1 has 3 partitions for 3 members: a allocation of 1 each. A holds 0 and 5, but 5 does not
        // exist, so A only counts as holding 0, which it keeps: every partition is assigned
        // exactly once and 5 disappears. A gets a new set and a new map, B and C are unchanged.
        SubscribedTopicDescriber describer = describer(3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), holding(T1, 0, 5)));
        members.put(B, member(Set.of(T1), holding(T1, 1)));
        members.put(C, member(Set.of(T1), holding(T1, 2)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(0)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(1)), partitions(result, B));
        assertEquals(Map.of(T1, Set.of(2)), partitions(result, C));
        assertNotSame(members.get(A).partitions(), partitions(result, A));
        assertNotSame(members.get(A).partitions().get(T1), partitions(result, A).get(T1));
        assertSame(members.get(B).partitions(), partitions(result, B));
        assertSame(members.get(C).partitions(), partitions(result, C));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testTopicWithoutPartitionsIsStale() {
        // T1 has 2 partitions and T2 has none. A holds a partition of T2 which is stale: A gets
        // a new map without T2, in which its T1 set is still the same instance. B is unchanged.
        SubscribedTopicDescriber describer = describer(2, 0);
        assertEquals(0, describer.numPartitions(T2));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0), T2, Set.of(0)))));
        members.put(B, member(Set.of(T1, T2), holding(T1, 1)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(0)), partitions(result, A));
        assertNotSame(members.get(A).partitions(), partitions(result, A));
        assertSame(members.get(A).partitions().get(T1), partitions(result, A).get(T1));
        assertSame(members.get(B).partitions(), partitions(result, B));
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testChangedTopicGivesANewMapWhileUnchangedTopicsKeepTheirSets() {
        // T1 has 3 partitions and T2 has 2, for 2 members. A holds all of T1, more than the base
        // of 1, and claims its extra partition: allocation 2, it keeps 0 and 1 and releases 2 to B.
        // T2 is settled with one partition each. Both members get a new map since their T1
        // partitions change, but their T2 sets are the very same instances.
        SubscribedTopicDescriber describer = describer(3, 2);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1, 2), T2, Set.of(0)))));
        members.put(B, member(Set.of(T1, T2), holding(T2, 1)));

        GroupAssignment result = assign(members, describer);

        assertEquals(Map.of(T1, Set.of(0, 1), T2, Set.of(0)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(2), T2, Set.of(1)), partitions(result, B));
        for (String memberId : members.keySet()) {
            assertNotSame(members.get(memberId).partitions(), partitions(result, memberId));
            assertSame(members.get(memberId).partitions().get(T2), partitions(result, memberId).get(T2));
        }
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testMemberHoldingAndReceivingNothingHasAnEmptyAssignment() {
        // T1 has 2 partitions for 3 members: a base of 0 and two extra partitions, claimed by A
        // and B which hold one partition each. C holds nothing and receives nothing.
        SubscribedTopicDescriber describer = describer(2);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), holding(T1, 0)));
        members.put(B, member(Set.of(T1), holding(T1, 1)));
        members.put(C, member(Set.of(T1), Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(A, B, C), result.members().keySet());
        assertEquals(Map.of(T1, Set.of(0)), partitions(result, A));
        assertEquals(Map.of(T1, Set.of(1)), partitions(result, B));
        assertTrue(partitions(result, C).isEmpty());
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testMemberWithoutSubscriptionHasAnEmptyAssignment() {
        // A subscribes to T1 with 2 partitions and B to nothing: the group is heterogeneous, A
        // gets both partitions and B an empty assignment.
        SubscribedTopicDescriber describer = describer(2);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), Assignment.EMPTY));
        members.put(B, member(Set.of(), Assignment.EMPTY));

        GroupAssignment result = assign(members, describer);

        assertEquals(Set.of(A, B), result.members().keySet());
        assertEquals(Map.of(T1, Set.of(0, 1)), partitions(result, A));
        assertTrue(partitions(result, B).isEmpty());
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testPartitionHeldBySeveralMembersIsRejected() {
        // T1 has 3 partitions for 3 members: a allocation of 1 each. A holds 0 and 1, and B holds 0
        // too. A keeps 0, its lowest, and so does B, so C receives 1 and partition 2 is left
        // without a member: the inconsistency is reported rather than silently producing an
        // incomplete assignment or failing on an array bound.
        SubscribedTopicDescriber describer = describer(3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(A, member(Set.of(T1), holding(T1, 0, 1)));
        members.put(B, member(Set.of(T1), holding(T1, 0)));
        members.put(C, member(Set.of(T1), Assignment.EMPTY));

        assertThrows(PartitionAssignorException.class, () -> assign(members, describer));
    }
}
