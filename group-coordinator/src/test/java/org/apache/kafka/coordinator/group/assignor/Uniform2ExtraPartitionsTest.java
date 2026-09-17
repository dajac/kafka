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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.maxBitsetBits;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.Uniform2TestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Uniform2ExtraPartitionsTest {
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
     * @param bitsets Whether the model uses bitsets, see {@link Uniform2GroupModel#MAX_BITSET_BITS}.
     * @return Members A, B and C without current partitions, subscribed to topic 1 with 5
     *         partitions, topic 2 with 4 and topic 3 with 8: bases 1, 1 and 2 and extra
     *         partitions 2, 1 and 2.
     */
    private static Uniform2GroupModel model(boolean bitsets) {
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 5, 4, 2)
            .addTopic(TOPIC_2, "topic-2", 4, 4, 2)
            .addTopic(TOPIC_3, "topic-3", 8, 4, 2)
            .buildDescriber();
        return new Uniform2GroupModel(spec(members(3, Set.of(TOPIC_1, TOPIC_2, TOPIC_3))), describer, false, maxBitsetBits(bitsets));
    }

    @Test
    public void testModelOfTheTests() {
        Uniform2GroupModel model = model(true);
        assertTrue(model.usesBitsets);
        assertFalse(model(false).usesBitsets);
        assertEquals(3, model.memberCount);
        assertEquals(3, model.topicCount);
        assertArrayEquals(new int[] {1, 1, 2}, model.basePartitionCount);
        assertArrayEquals(new int[] {2, 1, 2}, model.extraPartitionCount);
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testNothingIsAssignedInitially(boolean bitset) {
        Uniform2GroupModel model = model(bitset);
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);
        for (int t = 0; t < model.topicCount; t++) {
            assertEquals(0, extras.recipientCount(t));
            for (int m = 0; m < model.memberCount; m++) {
                assertFalse(extras.has(m, t));
                assertEquals(model.basePartitionCount[t], extras.quota(m, t));
            }
        }
        for (int m = 0; m < model.memberCount; m++) {
            assertEquals(0, extras.countOf(m));
            assertEquals(0, extras.freeCountOf(m));
        }
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testAddGivesAnExtraPartitionToTheMember(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(A, T1);

        assertTrue(extras.has(A, T1));
        assertFalse(extras.has(B, T1));
        assertFalse(extras.has(A, T2));
        assertEquals(1, extras.recipientCount(T1));
        assertEquals(A, extras.recipientAt(T1, 0));
        assertEquals(0, extras.recipientCount(T2));
        assertTopics(extras, A, T1);
        assertTopics(extras, B);
        assertEquals(2, extras.quota(A, T1));
        assertEquals(1, extras.quota(B, T1));
        assertEquals(1, extras.quota(A, T2));
        // Nothing is currently held, so the extra partition is free.
        assertEquals(1, extras.freeCountOf(A));
        assertEquals(0, extras.freeCountOf(B));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testSeveralMembersCanHoldExtraPartitionsOfTheSameTopic(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(C, T1);
        extras.add(A, T1);

        assertEquals(2, extras.recipientCount(T1));
        assertTrue(extras.has(A, T1));
        assertFalse(extras.has(B, T1));
        assertTrue(extras.has(C, T1));
        assertEquals(Set.of(A, C), Set.of(extras.recipientAt(T1, 0), extras.recipientAt(T1, 1)));
        assertTopics(extras, A, T1);
        assertTopics(extras, B);
        assertTopics(extras, C, T1);
        assertEquals(2, extras.quota(A, T1));
        assertEquals(1, extras.quota(B, T1));
        assertEquals(2, extras.quota(C, T1));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testSortRecipientsOrdersThemByMember(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(C, T3);
        extras.add(A, T3);
        extras.sortRecipients(T3);
        assertEquals(2, extras.recipientCount(T3));
        assertEquals(A, extras.recipientAt(T3, 0));
        assertEquals(C, extras.recipientAt(T3, 1));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testRemoveTakesTheExtraPartitionBack(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(A, T1);
        extras.add(B, T1);
        extras.remove(A, T1);

        assertFalse(extras.has(A, T1));
        assertTrue(extras.has(B, T1));
        assertEquals(1, extras.recipientCount(T1));
        assertEquals(B, extras.recipientAt(T1, 0));
        assertTopics(extras, A);
        assertTopics(extras, B, T1);
        assertEquals(1, extras.quota(A, T1));
        assertEquals(2, extras.quota(B, T1));
        assertEquals(0, extras.freeCountOf(A));
        assertEquals(1, extras.freeCountOf(B));

        extras.remove(B, T1);
        assertEquals(0, extras.recipientCount(T1));
        assertFalse(extras.has(B, T1));
        assertTopics(extras, B);
        assertEquals(0, extras.freeCountOf(B));

        // The extra partitions can be given again.
        extras.add(C, T1);
        assertEquals(1, extras.recipientCount(T1));
        assertEquals(C, extras.recipientAt(T1, 0));
        assertTopics(extras, C, T1);
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testRemoveARecipientFromTheMiddle(boolean bitset) {
        // Four members and topic 1 with 7 partitions: base 1 and three extra partitions.
        SubscribedTopicDescriber describer = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 7, 4, 2)
            .buildDescriber();
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members(4, Set.of(TOPIC_1))), describer, false, maxBitsetBits(bitset));
        assertArrayEquals(new int[] {3}, model.extraPartitionCount);

        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);
        extras.add(A, T1);
        extras.add(B, T1);
        extras.add(C, T1);
        extras.remove(B, T1);
        assertEquals(2, extras.recipientCount(T1));
        assertFalse(extras.has(B, T1));
        extras.sortRecipients(T1);
        assertEquals(A, extras.recipientAt(T1, 0));
        assertEquals(C, extras.recipientAt(T1, 1));

        extras.add(D, T1);
        assertEquals(3, extras.recipientCount(T1));
        extras.sortRecipients(T1);
        assertEquals(A, extras.recipientAt(T1, 0));
        assertEquals(C, extras.recipientAt(T1, 1));
        assertEquals(D, extras.recipientAt(T1, 2));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testTopicsOfStaysSortedWhenTopicsAreAddedOutOfOrder(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(A, T3);
        assertTopics(extras, A, T3);
        extras.add(A, T1);
        assertTopics(extras, A, T1, T3);
        extras.add(A, T2);
        assertTopics(extras, A, T1, T2, T3);

        assertTrue(extras.has(A, T1));
        assertTrue(extras.has(A, T2));
        assertTrue(extras.has(A, T3));
        assertEquals(2, extras.quota(A, T1));
        assertEquals(2, extras.quota(A, T2));
        assertEquals(3, extras.quota(A, T3));
        assertEquals(3, extras.freeCountOf(A));
        assertTopics(extras, B);
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testRemoveFromTheMiddleOfTheTopics(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        extras.add(A, T1);
        extras.add(A, T2);
        extras.add(A, T3);

        extras.remove(A, T2);
        assertTopics(extras, A, T1, T3);
        assertTrue(extras.has(A, T1));
        assertFalse(extras.has(A, T2));
        assertTrue(extras.has(A, T3));
        assertEquals(0, extras.recipientCount(T2));
        assertEquals(1, extras.recipientCount(T1));
        assertEquals(1, extras.recipientCount(T3));
        assertEquals(1, extras.quota(A, T2));
        assertEquals(2, extras.freeCountOf(A));

        extras.remove(A, T1);
        assertTopics(extras, A, T3);
        assertFalse(extras.has(A, T1));
        assertTrue(extras.has(A, T3));

        extras.remove(A, T3);
        assertTopics(extras, A);
        assertEquals(0, extras.freeCountOf(A));
        for (int t = 0; t < 3; t++) {
            assertFalse(extras.has(A, t));
            assertEquals(0, extras.recipientCount(t));
        }
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testMemberTopicsGrowBeyondTheInitialCapacity(boolean bitset) {
        // Six topics with 3 partitions each for two members: base 1 and one extra partition each.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int t = 0; t < 6; t++) {
            Uuid topicId = new Uuid(1L, 1L + t);
            builder.addTopic(topicId, "topic-" + t, 3, 4, 2);
            topics.add(topicId);
        }
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members(2, topics)), builder.buildDescriber(), false, maxBitsetBits(bitset));
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1}, model.extraPartitionCount);

        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);
        for (int t = 5; t >= 0; t--) {
            extras.add(A, t);
        }
        assertTopics(extras, A, 0, 1, 2, 3, 4, 5);
        assertTopics(extras, B);
        for (int t = 0; t < 6; t++) {
            assertTrue(extras.has(A, t));
            assertFalse(extras.has(B, t));
            assertEquals(2, extras.quota(A, t));
            assertEquals(1, extras.quota(B, t));
            assertEquals(1, extras.recipientCount(t));
            assertEquals(A, extras.recipientAt(t, 0));
        }
        assertEquals(6, extras.freeCountOf(A));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testFreeCountOnlyCountsExtraPartitionsWithoutABackingCurrentPartition(boolean bitset) {
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
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members), describer, false, maxBitsetBits(bitset));
        assertArrayEquals(new int[] {1, 1}, model.basePartitionCount);
        assertArrayEquals(new int[] {2, 1}, model.extraPartitionCount);
        assertTrue(model.isBacked(A, T1));
        assertFalse(model.isBacked(A, T2));
        assertFalse(model.isBacked(B, T1));
        assertTrue(model.isBacked(B, T2));
        assertFalse(model.isBacked(C, T1));
        assertFalse(model.isBacked(C, T2));

        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);

        // Backed.
        extras.add(A, T1);
        assertEquals(1, extras.countOf(A));
        assertEquals(0, extras.freeCountOf(A));

        // Free.
        extras.add(A, T2);
        assertEquals(2, extras.countOf(A));
        assertEquals(1, extras.freeCountOf(A));

        // Free.
        extras.add(B, T1);
        assertEquals(1, extras.countOf(B));
        assertEquals(1, extras.freeCountOf(B));

        extras.remove(B, T1);
        assertEquals(0, extras.countOf(B));
        assertEquals(0, extras.freeCountOf(B));

        extras.remove(A, T2);
        assertEquals(1, extras.countOf(A));
        assertEquals(0, extras.freeCountOf(A));

        // Backed.
        extras.add(B, T2);
        assertEquals(1, extras.countOf(B));
        assertEquals(0, extras.freeCountOf(B));

        // Free.
        extras.add(C, T1);
        assertEquals(1, extras.countOf(C));
        assertEquals(1, extras.freeCountOf(C));

        extras.remove(A, T1);
        assertEquals(0, extras.countOf(A));
        assertEquals(0, extras.freeCountOf(A));
        assertEquals(1, extras.recipientCount(T1));
        assertEquals(C, extras.recipientAt(T1, 0));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testQuotaIsTheBasePlusOneWithAnExtraPartition(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        assertEquals(1, extras.quota(A, T1));
        assertEquals(2, extras.quota(A, T3));

        extras.add(A, T1);
        extras.add(A, T3);
        assertEquals(2, extras.quota(A, T1));
        assertEquals(3, extras.quota(A, T3));
        assertEquals(1, extras.quota(B, T1));
        assertEquals(2, extras.quota(B, T3));

        extras.remove(A, T3);
        assertEquals(2, extras.quota(A, T1));
        assertEquals(2, extras.quota(A, T3));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testCountInRack(boolean bitset) {
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
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members), describer, true, maxBitsetBits(bitset));
        assertTrue(model.usesRacks);
        assertEquals(2, model.rackCount);
        assertArrayEquals(new int[] {0, 1, 0}, model.memberRack);

        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);
        assertEquals(0, extras.countInRack(T1, 0));
        assertEquals(0, extras.countInRack(T1, 1));

        extras.add(A, T1);
        assertEquals(1, extras.countInRack(T1, 0));
        assertEquals(0, extras.countInRack(T1, 1));
        assertEquals(0, extras.countInRack(T2, 0));

        extras.add(B, T1);
        assertEquals(1, extras.countInRack(T1, 0));
        assertEquals(1, extras.countInRack(T1, 1));

        extras.add(C, T2);
        assertEquals(1, extras.countInRack(T2, 0));
        assertEquals(0, extras.countInRack(T2, 1));

        extras.remove(A, T1);
        assertEquals(0, extras.countInRack(T1, 0));
        assertEquals(1, extras.countInRack(T1, 1));

        extras.add(C, T1);
        assertEquals(1, extras.countInRack(T1, 0));
        assertEquals(1, extras.countInRack(T1, 1));
        assertEquals(2, extras.recipientCount(T1));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testAddRejectsMoreRecipientsThanExtraPartitions(boolean bitset) {
        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model(bitset));
        // Topic 2 has a single extra partition.
        extras.add(A, T2);

        assertThrows(IllegalStateException.class, () -> extras.add(B, T2));
        assertEquals(1, extras.recipientCount(T2));
        assertTrue(extras.has(A, T2));
        assertFalse(extras.has(B, T2));
    }

    @ParameterizedTest(name = "bitset={0}")
    @ValueSource(booleans = {false, true})
    public void testHasWithMoreThanSixtyFourMemberTopicPairs(boolean bitset) {
        // Thirty topics with 5 partitions each for three members: base 1 and two extra partitions
        // each, 90 member and topic pairs, more than a single word of the bitset holds.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int t = 0; t < 30; t++) {
            Uuid topicId = new Uuid(1L, 1L + t);
            builder.addTopic(topicId, "topic-" + t, 5, 4, 2);
            topics.add(topicId);
        }
        Uniform2GroupModel model = new Uniform2GroupModel(spec(members(3, topics)), builder.buildDescriber(), false, maxBitsetBits(bitset));
        assertEquals(30, model.topicCount);

        Uniform2ExtraPartitions extras = new Uniform2ExtraPartitions(model);
        // With the topic first and three members, topic 21 of A is pair 63, the last of the
        // first word of the bitset, and topic 21 of B is pair 64, the first of the second word.
        extras.add(A, 21);
        extras.add(B, 21);
        extras.add(C, 29);
        extras.add(A, 0);
        extras.add(B, 20);
        extras.add(C, 22);
        Set<Integer> expected = Set.of(A * 100 + 21, B * 100 + 21, C * 100 + 29, A * 100, B * 100 + 20, C * 100 + 22);
        for (int m = 0; m < model.memberCount; m++) {
            for (int t = 0; t < model.topicCount; t++) {
                assertEquals(expected.contains(m * 100 + t), extras.has(m, t), "member " + m + " topic " + t);
            }
        }
        assertTopics(extras, A, 0, 21);
        assertTopics(extras, B, 20, 21);
        assertTopics(extras, C, 22, 29);

        // Taking back pair 64 leaves pair 63 and pair 65 alone.
        extras.remove(B, 21);
        assertFalse(extras.has(B, 21));
        assertTrue(extras.has(A, 21));
        assertTrue(extras.has(C, 22));
        assertTopics(extras, B, 20);
        assertEquals(1, extras.recipientCount(21));
        assertEquals(A, extras.recipientAt(21, 0));
    }

    /**
     * Checks the number of extra partitions of the member and its topics, which are the first
     * {@code countOf} entries of {@code topicsOf}.
     */
    private static void assertTopics(Uniform2ExtraPartitions extras, int member, int... expectedTopics) {
        assertEquals(expectedTopics.length, extras.countOf(member));
        if (expectedTopics.length > 0) {
            assertArrayEquals(expectedTopics, Arrays.copyOf(extras.topicsOf(member), expectedTopics.length));
        }
    }
}
