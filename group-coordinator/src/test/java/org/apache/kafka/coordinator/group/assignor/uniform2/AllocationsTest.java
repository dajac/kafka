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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllocationsTest {
    private static final Uuid TOPIC_1 = new Uuid(1L, 1L);
    private static final Uuid TOPIC_2 = new Uuid(1L, 2L);
    private static final Uuid TOPIC_3 = new Uuid(1L, 3L);
    // Topics and members are numbered in id order.
    private static final int T1 = 0;
    private static final int T2 = 1;
    private static final int T3 = 2;
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int D = 3;

    /**
     * @return Members A, B, ... without current partitions, subscribed to the topics.
     */
    private static Map<String, MemberSubscriptionAndAssignmentImpl> members(int count, Set<Uuid> topics) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int m = 0; m < count; m++) {
            members.put(String.valueOf((char) ('A' + m)), member(topics, Assignment.EMPTY));
        }
        return members;
    }

    /**
     * @return Members A, B and C without current partitions, subscribed to topic 1 with 5
     *         partitions, topic 2 with 4 and topic 3 with 8: bases 1, 1 and 2 and extra
     *         partitions 2, 1 and 2.
     */
    private static GroupModel model() {
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 5, 4, 2)
            .addTopic(TOPIC_2, "topic-2", 4, 4, 2)
            .addTopic(TOPIC_3, "topic-3", 8, 4, 2)
            .buildDescriber();
        return new GroupModel(spec(members(3, Set.of(TOPIC_1, TOPIC_2, TOPIC_3))), describer, false);
    }

    @Test
    public void testModelOfTheTests() {
        GroupModel model = model();
        assertEquals(3, model.memberCount());
        assertEquals(3, model.topicCount());
        assertArrayEquals(new int[] {1, 1, 2}, model.basePartitionCount());
        assertArrayEquals(new int[] {2, 1, 2}, model.extraPartitionCount());
    }

    @Test
    public void testNothingIsAssignedInitially() {
        GroupModel model = model();
        Allocations allocations = new Allocations(model);
        for (int t = 0; t < model.topicCount(); t++) {
            assertEquals(0, allocations.extraReceiverCount(t));
            for (int m = 0; m < model.memberCount(); m++) {
                assertFalse(allocations.hasExtra(m, t));
                assertEquals(model.basePartitionCount()[t], allocations.allocation(m, t));
            }
        }
        for (int m = 0; m < model.memberCount(); m++) {
            assertEquals(0, allocations.extraCount(m));
            assertEquals(0, allocations.freeExtraCount(m));
        }
    }

    @Test
    public void testAddGivesAnExtraPartitionToTheMember() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(A, T1);

        assertTrue(allocations.hasExtra(A, T1));
        assertFalse(allocations.hasExtra(B, T1));
        assertFalse(allocations.hasExtra(A, T2));
        assertEquals(1, allocations.extraReceiverCount(T1));
        assertEquals(A, allocations.extraReceiverAt(T1, 0));
        assertEquals(0, allocations.extraReceiverCount(T2));
        assertTopics(allocations, A, T1);
        assertTopics(allocations, B);
        assertEquals(2, allocations.allocation(A, T1));
        assertEquals(1, allocations.allocation(B, T1));
        assertEquals(1, allocations.allocation(A, T2));
        // Nothing is currently held, so the extra partition is free.
        assertEquals(1, allocations.freeExtraCount(A));
        assertEquals(0, allocations.freeExtraCount(B));
    }

    @Test
    public void testSeveralMembersCanHoldExtraPartitionsOfTheSameTopic() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(C, T1);
        allocations.addExtra(A, T1);

        assertEquals(2, allocations.extraReceiverCount(T1));
        assertTrue(allocations.hasExtra(A, T1));
        assertFalse(allocations.hasExtra(B, T1));
        assertTrue(allocations.hasExtra(C, T1));
        assertEquals(Set.of(A, C), Set.of(allocations.extraReceiverAt(T1, 0), allocations.extraReceiverAt(T1, 1)));
        assertTopics(allocations, A, T1);
        assertTopics(allocations, B);
        assertTopics(allocations, C, T1);
        assertEquals(2, allocations.allocation(A, T1));
        assertEquals(1, allocations.allocation(B, T1));
        assertEquals(2, allocations.allocation(C, T1));
    }

    @Test
    public void testSortReceiversOrdersThemByMember() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(C, T3);
        allocations.addExtra(A, T3);
        allocations.sortExtraReceivers(T3);
        assertEquals(2, allocations.extraReceiverCount(T3));
        assertEquals(A, allocations.extraReceiverAt(T3, 0));
        assertEquals(C, allocations.extraReceiverAt(T3, 1));
    }

    @Test
    public void testRemoveTakesTheExtraPartitionBack() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(A, T1);
        allocations.addExtra(B, T1);
        allocations.removeExtra(A, T1);

        assertFalse(allocations.hasExtra(A, T1));
        assertTrue(allocations.hasExtra(B, T1));
        assertEquals(1, allocations.extraReceiverCount(T1));
        assertEquals(B, allocations.extraReceiverAt(T1, 0));
        assertTopics(allocations, A);
        assertTopics(allocations, B, T1);
        assertEquals(1, allocations.allocation(A, T1));
        assertEquals(2, allocations.allocation(B, T1));
        assertEquals(0, allocations.freeExtraCount(A));
        assertEquals(1, allocations.freeExtraCount(B));

        allocations.removeExtra(B, T1);
        assertEquals(0, allocations.extraReceiverCount(T1));
        assertFalse(allocations.hasExtra(B, T1));
        assertTopics(allocations, B);
        assertEquals(0, allocations.freeExtraCount(B));

        // The extra partitions can be given again.
        allocations.addExtra(C, T1);
        assertEquals(1, allocations.extraReceiverCount(T1));
        assertEquals(C, allocations.extraReceiverAt(T1, 0));
        assertTopics(allocations, C, T1);
    }

    @Test
    public void testRemoveAReceiverFromTheMiddle() {
        // Four members and topic 1 with 7 partitions: base 1 and three extra partitions.
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 7, 4, 2)
            .buildDescriber();
        GroupModel model = new GroupModel(spec(members(4, Set.of(TOPIC_1))), describer, false);
        assertArrayEquals(new int[] {3}, model.extraPartitionCount());

        Allocations allocations = new Allocations(model);
        allocations.addExtra(A, T1);
        allocations.addExtra(B, T1);
        allocations.addExtra(C, T1);
        allocations.removeExtra(B, T1);
        assertEquals(2, allocations.extraReceiverCount(T1));
        assertFalse(allocations.hasExtra(B, T1));
        allocations.sortExtraReceivers(T1);
        assertEquals(A, allocations.extraReceiverAt(T1, 0));
        assertEquals(C, allocations.extraReceiverAt(T1, 1));

        allocations.addExtra(D, T1);
        assertEquals(3, allocations.extraReceiverCount(T1));
        allocations.sortExtraReceivers(T1);
        assertEquals(A, allocations.extraReceiverAt(T1, 0));
        assertEquals(C, allocations.extraReceiverAt(T1, 1));
        assertEquals(D, allocations.extraReceiverAt(T1, 2));
    }

    @Test
    public void testTopicsOfStaysSortedWhenTopicsAreAddedOutOfOrder() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(A, T3);
        assertTopics(allocations, A, T3);
        allocations.addExtra(A, T1);
        assertTopics(allocations, A, T1, T3);
        allocations.addExtra(A, T2);
        assertTopics(allocations, A, T1, T2, T3);

        assertTrue(allocations.hasExtra(A, T1));
        assertTrue(allocations.hasExtra(A, T2));
        assertTrue(allocations.hasExtra(A, T3));
        assertEquals(2, allocations.allocation(A, T1));
        assertEquals(2, allocations.allocation(A, T2));
        assertEquals(3, allocations.allocation(A, T3));
        assertEquals(3, allocations.freeExtraCount(A));
        assertTopics(allocations, B);
    }

    @Test
    public void testRemoveFromTheMiddleOfTheTopics() {
        Allocations allocations = new Allocations(model());
        allocations.addExtra(A, T1);
        allocations.addExtra(A, T2);
        allocations.addExtra(A, T3);

        allocations.removeExtra(A, T2);
        assertTopics(allocations, A, T1, T3);
        assertTrue(allocations.hasExtra(A, T1));
        assertFalse(allocations.hasExtra(A, T2));
        assertTrue(allocations.hasExtra(A, T3));
        assertEquals(0, allocations.extraReceiverCount(T2));
        assertEquals(1, allocations.extraReceiverCount(T1));
        assertEquals(1, allocations.extraReceiverCount(T3));
        assertEquals(1, allocations.allocation(A, T2));
        assertEquals(2, allocations.freeExtraCount(A));

        allocations.removeExtra(A, T1);
        assertTopics(allocations, A, T3);
        assertFalse(allocations.hasExtra(A, T1));
        assertTrue(allocations.hasExtra(A, T3));

        allocations.removeExtra(A, T3);
        assertTopics(allocations, A);
        assertEquals(0, allocations.freeExtraCount(A));
        for (int t = 0; t < 3; t++) {
            assertFalse(allocations.hasExtra(A, t));
            assertEquals(0, allocations.extraReceiverCount(t));
        }
    }

    @Test
    public void testMemberTopicsGrowBeyondTheInitialCapacity() {
        // Six topics with 3 partitions each for two members: base 1 and one extra partition each.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int t = 0; t < 6; t++) {
            Uuid topicId = new Uuid(1L, 1L + t);
            builder.addTopic(topicId, "topic-" + t, 3, 4, 2);
            topics.add(topicId);
        }
        GroupModel model = new GroupModel(spec(members(2, topics)), builder.buildDescriber(), false);
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1}, model.extraPartitionCount());

        Allocations allocations = new Allocations(model);
        for (int t = 5; t >= 0; t--) {
            allocations.addExtra(A, t);
        }
        assertTopics(allocations, A, 0, 1, 2, 3, 4, 5);
        assertTopics(allocations, B);
        for (int t = 0; t < 6; t++) {
            assertTrue(allocations.hasExtra(A, t));
            assertFalse(allocations.hasExtra(B, t));
            assertEquals(2, allocations.allocation(A, t));
            assertEquals(1, allocations.allocation(B, t));
            assertEquals(1, allocations.extraReceiverCount(t));
            assertEquals(A, allocations.extraReceiverAt(t, 0));
        }
        assertEquals(6, allocations.freeExtraCount(A));
    }

    @Test
    public void testFreeCountOnlyCountsExtraPartitionsWithoutABackingCurrentPartition() {
        // Topic 1 with 5 partitions and topic 2 with 4 for three members: base 1 for both. A
        // holds three partitions of topic 1, more than the base, so an extra partition of topic
        // 1 is backed for A. B holds two partitions of topic 2, so an extra partition of topic 2
        // is backed for B. Every other extra partition is free.
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 5, 4, 2)
            .addTopic(TOPIC_2, "topic-2", 4, 4, 2)
            .buildDescriber();
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(TOPIC_1, TOPIC_2), new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 0, 1, 2)))));
        members.put("B", member(Set.of(TOPIC_1, TOPIC_2), new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 3), mkTopicAssignment(TOPIC_2, 0, 1)))));
        members.put("C", member(Set.of(TOPIC_1, TOPIC_2), new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 4), mkTopicAssignment(TOPIC_2, 2)))));
        GroupModel model = new GroupModel(spec(members), describer, false);
        assertArrayEquals(new int[] {1, 1}, model.basePartitionCount());
        assertArrayEquals(new int[] {2, 1}, model.extraPartitionCount());
        assertTrue(model.isBacked(A, T1));
        assertFalse(model.isBacked(A, T2));
        assertFalse(model.isBacked(B, T1));
        assertTrue(model.isBacked(B, T2));
        assertFalse(model.isBacked(C, T1));
        assertFalse(model.isBacked(C, T2));

        Allocations allocations = new Allocations(model);

        // Backed.
        allocations.addExtra(A, T1);
        assertEquals(1, allocations.extraCount(A));
        assertEquals(0, allocations.freeExtraCount(A));

        // Free.
        allocations.addExtra(A, T2);
        assertEquals(2, allocations.extraCount(A));
        assertEquals(1, allocations.freeExtraCount(A));

        // Free.
        allocations.addExtra(B, T1);
        assertEquals(1, allocations.extraCount(B));
        assertEquals(1, allocations.freeExtraCount(B));

        allocations.removeExtra(B, T1);
        assertEquals(0, allocations.extraCount(B));
        assertEquals(0, allocations.freeExtraCount(B));

        allocations.removeExtra(A, T2);
        assertEquals(1, allocations.extraCount(A));
        assertEquals(0, allocations.freeExtraCount(A));

        // Backed.
        allocations.addExtra(B, T2);
        assertEquals(1, allocations.extraCount(B));
        assertEquals(0, allocations.freeExtraCount(B));

        // Free.
        allocations.addExtra(C, T1);
        assertEquals(1, allocations.extraCount(C));
        assertEquals(1, allocations.freeExtraCount(C));

        allocations.removeExtra(A, T1);
        assertEquals(0, allocations.extraCount(A));
        assertEquals(0, allocations.freeExtraCount(A));
        assertEquals(1, allocations.extraReceiverCount(T1));
        assertEquals(C, allocations.extraReceiverAt(T1, 0));
    }

    @Test
    public void testAllocationIsTheBasePlusOneWithAnExtraPartition() {
        Allocations allocations = new Allocations(model());
        assertEquals(1, allocations.allocation(A, T1));
        assertEquals(2, allocations.allocation(A, T3));

        allocations.addExtra(A, T1);
        allocations.addExtra(A, T3);
        assertEquals(2, allocations.allocation(A, T1));
        assertEquals(3, allocations.allocation(A, T3));
        assertEquals(1, allocations.allocation(B, T1));
        assertEquals(2, allocations.allocation(B, T3));

        allocations.removeExtra(A, T3);
        assertEquals(2, allocations.allocation(A, T1));
        assertEquals(2, allocations.allocation(A, T3));
    }

    @Test
    public void testCountInRack() {
        // Members A and C in rack 0 and B in rack 1, topic 1 with 5 partitions and topic 2 with
        // 4: two and one extra partitions.
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addBroker(0, "rack-0")
            .addBroker(1, "rack-1")
            .addTopic(TOPIC_1, "topic-1", 5, 2, 2)
            .addTopic(TOPIC_2, "topic-2", 4, 2, 2)
            .buildDescriber();
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("rack-0", Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        members.put("B", member("rack-1", Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        members.put("C", member("rack-0", Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        GroupModel model = new GroupModel(spec(members), describer, true);
        assertTrue(model.usesRacks());
        assertEquals(2, model.racks().count());
        assertArrayEquals(new int[] {0, 1, 0}, model.racks().memberRack());

        Allocations allocations = new Allocations(model);
        assertEquals(0, allocations.extraCountInRack(T1, 0));
        assertEquals(0, allocations.extraCountInRack(T1, 1));

        allocations.addExtra(A, T1);
        assertEquals(1, allocations.extraCountInRack(T1, 0));
        assertEquals(0, allocations.extraCountInRack(T1, 1));
        assertEquals(0, allocations.extraCountInRack(T2, 0));

        allocations.addExtra(B, T1);
        assertEquals(1, allocations.extraCountInRack(T1, 0));
        assertEquals(1, allocations.extraCountInRack(T1, 1));

        allocations.addExtra(C, T2);
        assertEquals(1, allocations.extraCountInRack(T2, 0));
        assertEquals(0, allocations.extraCountInRack(T2, 1));

        allocations.removeExtra(A, T1);
        assertEquals(0, allocations.extraCountInRack(T1, 0));
        assertEquals(1, allocations.extraCountInRack(T1, 1));

        allocations.addExtra(C, T1);
        assertEquals(1, allocations.extraCountInRack(T1, 0));
        assertEquals(1, allocations.extraCountInRack(T1, 1));
        assertEquals(2, allocations.extraReceiverCount(T1));
    }

    @Test
    public void testAddRejectsMoreReceiversThanExtraPartitions() {
        Allocations allocations = new Allocations(model());
        // Topic 2 has a single extra partition.
        allocations.addExtra(A, T2);

        assertThrows(IllegalStateException.class, () -> allocations.addExtra(B, T2));
        assertEquals(1, allocations.extraReceiverCount(T2));
        assertTrue(allocations.hasExtra(A, T2));
        assertFalse(allocations.hasExtra(B, T2));
    }

    @Test
    public void testHasWithMoreThanSixtyFourMemberTopicPairs() {
        // Thirty topics with 5 partitions each for three members: base 1 and two extra partitions
        // each, 90 member and topic pairs, more than a single word of the bitset holds.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int t = 0; t < 30; t++) {
            Uuid topicId = new Uuid(1L, 1L + t);
            builder.addTopic(topicId, "topic-" + t, 5, 4, 2);
            topics.add(topicId);
        }
        GroupModel model = new GroupModel(spec(members(3, topics)), builder.buildDescriber(), false);
        assertEquals(30, model.topicCount());

        Allocations allocations = new Allocations(model);
        // With the topic first and three members, topic 21 of A is pair 63, the last of the
        // first word of the bitset, and topic 21 of B is pair 64, the first of the second word.
        allocations.addExtra(A, 21);
        allocations.addExtra(B, 21);
        allocations.addExtra(C, 29);
        allocations.addExtra(A, 0);
        allocations.addExtra(B, 20);
        allocations.addExtra(C, 22);
        Set<Integer> expected = Set.of(A * 100 + 21, B * 100 + 21, C * 100 + 29, A * 100, B * 100 + 20, C * 100 + 22);
        for (int m = 0; m < model.memberCount(); m++) {
            for (int t = 0; t < model.topicCount(); t++) {
                assertEquals(expected.contains(m * 100 + t), allocations.hasExtra(m, t), "member " + m + " topic " + t);
            }
        }
        assertTopics(allocations, A, 0, 21);
        assertTopics(allocations, B, 20, 21);
        assertTopics(allocations, C, 22, 29);

        // Taking back pair 64 leaves pair 63 and pair 65 alone.
        allocations.removeExtra(B, 21);
        assertFalse(allocations.hasExtra(B, 21));
        assertTrue(allocations.hasExtra(A, 21));
        assertTrue(allocations.hasExtra(C, 22));
        assertTopics(allocations, B, 20);
        assertEquals(1, allocations.extraReceiverCount(21));
        assertEquals(A, allocations.extraReceiverAt(21, 0));
    }

    /**
     * Checks the number of extra partitions of the member and its topics, which are the first
     * {@code countOf} entries of {@code topicsOf}.
     */
    private static void assertTopics(Allocations allocations, int member, int... expectedTopics) {
        assertEquals(expectedTopics.length, allocations.extraCount(member));
        if (expectedTopics.length > 0) {
            assertArrayEquals(expectedTopics, Arrays.copyOf(allocations.topicsOf(member), expectedTopics.length));
        }
    }
}
