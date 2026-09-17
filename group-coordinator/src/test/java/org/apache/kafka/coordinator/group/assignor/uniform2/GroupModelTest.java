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
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.TopicIndex.NONE;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupModelTest {
    // Topics and members are sorted by id, so these ids fix the indices: T1 is topic 0, T2 is
    // topic 1 and T3 is topic 2, while A is member 0, B is member 1, and so on.
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(1L, 2L);
    private static final Uuid T3 = new Uuid(1L, 3L);
    private static final Uuid UNKNOWN_TOPIC = new Uuid(1L, 4L);
    private static final Uuid[] TOPICS = {T1, T2, T3};

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
     * @return A describer with brokers 0 and 2 in rack r1, broker 1 in rack r2 and broker 3 in rack r3.
     *         T1 has 4 partitions with replicas in r1, r2, both and r3 respectively; T2 has 2 partitions
     *         with replicas in r1 and r3, then r2 and r3.
     */
    private static SubscribedTopicDescriber rackDescriber() {
        return new TestMetadataImageBuilder()
            .addBroker(0, "r1")
            .addBroker(1, "r2")
            .addBroker(2, "r1")
            .addBroker(3, "r3")
            .addTopic(T1, "topic-1", List.of(List.of(0), List.of(1), List.of(2, 1), List.of(3)))
            .addTopic(T2, "topic-2", List.of(List.of(0, 3), List.of(1, 3)))
            .buildDescriber();
    }

    private static GroupModel model(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        boolean rackAwareEnabled
    ) {
        return new GroupModel(spec(members), describer, rackAwareEnabled);
    }

    @Test
    public void testMembersAndTopicsAreSortedAndNumbered() {
        // The insertion order of the members and topics differs from their id order on purpose.
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new LinkedHashMap<>();
        members.put("C", member(Set.of(T3, T1, T2), Assignment.EMPTY));
        members.put("A", member(Set.of(T3, T1, T2), Assignment.EMPTY));
        members.put("B", member(Set.of(T3, T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, describer(1, 1, 1), false);

        assertEquals(3, model.memberCount());
        assertArrayEquals(new String[]{"A", "B", "C"}, model.memberIds());
        assertEquals(3, model.topicCount());
        assertArrayEquals(new Uuid[]{T1, T2, T3}, model.topicIds());
        assertEquals(0, model.topicIndex().indexOf(T1));
        assertEquals(1, model.topicIndex().indexOf(T2));
        assertEquals(2, model.topicIndex().indexOf(T3));
        assertEquals(NONE, model.topicIndex().indexOf(UNKNOWN_TOPIC));
    }

    @Test
    public void testBaseAndExtraPartitions() {
        // T1: 7 / 3 = 2 base partitions and 7 % 3 = 1 extra partition.
        // T2: fewer partitions than subscribers, so 0 base partitions and 2 extra partitions.
        // T3: no partitions at all.
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        members.put("B", member(Set.of(T1, T2, T3), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2, T3), Assignment.EMPTY));

        GroupModel model = model(members, describer(7, 2, 0), false);

        assertArrayEquals(new int[]{7, 2, 0}, model.partitionCounts());
        assertArrayEquals(new int[]{2, 0, 0}, model.basePartitionCount());
        assertArrayEquals(new int[]{1, 2, 0}, model.extraPartitionCount());
        assertEquals(7, model.maxPartitionsPerTopic());
    }

    @Test
    public void testSubscribedTopicMissingFromDescriberThrows() {
        SubscribedTopicDescriber describer = describer(3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), Assignment.EMPTY));

        assertThrows(PartitionAssignorException.class, () -> model(members, describer, false));
    }

    @Test
    public void testHomogeneousSubscriptions() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, describer(5, 4), false);

        assertTrue(model.homogeneous());
        // Every topic has every member and every member has every topic, sharing one array each.
        assertArrayEquals(new int[]{0, 1, 2}, model.subscribers()[0]);
        assertSame(model.subscribers()[0], model.subscribers()[1]);
        assertArrayEquals(new int[]{0, 1}, model.memberTopics()[0]);
        assertSame(model.memberTopics()[0], model.memberTopics()[1]);
        assertSame(model.memberTopics()[0], model.memberTopics()[2]);
        for (int m = 0; m < 3; m++) {
            for (int t = 0; t < 2; t++) {
                assertTrue(model.isSubscribed(m, t));
            }
        }

        // One cohort with every member, whose base load is 5 / 3 + 4 / 3 = 2.
        assertEquals(1, model.cohorts().count());
        assertArrayEquals(new int[]{0, 0, 0}, model.cohorts().memberCohort());
        assertArrayEquals(new int[]{3}, model.cohorts().size());
        assertArrayEquals(new int[]{2}, model.cohorts().baseLoad());
        assertArrayEquals(new int[]{0, 1}, model.cohorts().topics()[0]);
        assertArrayEquals(new int[]{0}, model.cohorts().rack());
        assertArrayEquals(new int[]{0, 1, 2}, model.cohorts().topicCohortStart());
        assertArrayEquals(new int[]{0, 0}, model.cohorts().topicCohorts());
        assertEquals(1, model.maxCohortsPerTopic());
        assertFalse(model.usesRacks());
    }

    @Test
    public void testHeterogeneousSubscriptions() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member(Set.of(T2, T3), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2), Assignment.EMPTY));
        members.put("D", member(Set.of(T3), Assignment.EMPTY));

        GroupModel model = model(members, describer(4, 6, 3), false);

        assertFalse(model.homogeneous());
        // Subscribers per topic and topics per member, both ascending.
        assertArrayEquals(new int[]{0, 2}, model.subscribers()[0]);
        assertArrayEquals(new int[]{0, 1, 2}, model.subscribers()[1]);
        assertArrayEquals(new int[]{1, 3}, model.subscribers()[2]);
        assertArrayEquals(new int[]{0, 1}, model.memberTopics()[0]);
        assertArrayEquals(new int[]{1, 2}, model.memberTopics()[1]);
        assertArrayEquals(new int[]{0, 1}, model.memberTopics()[2]);
        assertArrayEquals(new int[]{2}, model.memberTopics()[3]);
        assertTrue(model.isSubscribed(1, 1));
        assertFalse(model.isSubscribed(1, 0));
        assertTrue(model.isSubscribed(3, 2));
        assertFalse(model.isSubscribed(3, 1));

        // T1: 4 / 2 = 2 base partitions, T2: 6 / 3 = 2, T3: 3 / 2 = 1 with one extra partition.
        assertArrayEquals(new int[]{2, 2, 1}, model.basePartitionCount());
        assertArrayEquals(new int[]{0, 0, 1}, model.extraPartitionCount());

        // Cohorts are numbered in the order of their first member: {A, C} on T1 and T2 with base
        // load 2 + 2, {B} on T2 and T3 with base load 2 + 1, {D} on T3 with base load 1.
        assertEquals(3, model.cohorts().count());
        assertArrayEquals(new int[]{0, 1, 0, 2}, model.cohorts().memberCohort());
        assertArrayEquals(new int[]{0, 1}, model.cohorts().topics()[0]);
        assertArrayEquals(new int[]{1, 2}, model.cohorts().topics()[1]);
        assertArrayEquals(new int[]{2}, model.cohorts().topics()[2]);
        assertArrayEquals(new int[]{4, 3, 1}, model.cohorts().baseLoad());
        assertArrayEquals(new int[]{2, 1, 1}, model.cohorts().size());
        assertArrayEquals(new int[]{0, 0, 0}, model.cohorts().rack());

        // T1 has cohort 0, T2 has cohorts 0 and 1, T3 has cohorts 1 and 2.
        assertArrayEquals(new int[]{0, 1, 3, 5}, model.cohorts().topicCohortStart());
        assertArrayEquals(new int[]{0, 0, 1, 1, 2}, model.cohorts().topicCohorts());
        assertEquals(2, model.maxCohortsPerTopic());
        assertEquals(6, model.maxPartitionsPerTopic());
    }

    @Test
    public void testCurrentOwners() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(0, 1), T2, Set.of(0)))));
        // B is not subscribed to T1: its partitions of T1 are stale.
        members.put("B", member(Set.of(T2), new Assignment(Map.of(T1, Set.of(2), T2, Set.of(1, 2)))));
        // An empty set and a topic unknown to the group are stale.
        members.put("C", member(Set.of(T1, T2), new Assignment(Map.of(T1, Set.of(), T3, Set.of(0)))));
        members.put("D", member(Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, describer(4, 4), false);

        // Owners per topic in ascending member order: T1 has A, T2 has A and B.
        assertArrayEquals(new int[]{0, 1, 3}, model.owners().start());
        assertArrayEquals(new int[]{0, 0, 1}, model.owners().member());
        assertSame(members.get("A").partitions().get(T1), model.owners().partitions()[0]);
        assertSame(members.get("A").partitions().get(T2), model.owners().partitions()[1]);
        assertSame(members.get("B").partitions().get(T2), model.owners().partitions()[2]);
        assertArrayEquals(new boolean[]{false, true, true, false}, model.hasStalePartitions());
        for (int m = 0; m < model.memberCount(); m++) {
            assertSame(members.get(model.memberIds()[m]).partitions(), model.currentAssignments()[m]);
        }
    }

    @Test
    public void testBackedTopics() {
        // With four members, T1 has 5 / 4 = 1 base partition, T2 has 4 / 4 = 1 and T3 has 1 / 4 = 0.
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        // A owns more than the base partitions of both T1 and T2.
        members.put("A", member(Set.of(T1, T2, T3), new Assignment(Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1)))));
        // B owns exactly the base partitions of T1, and more than the base partitions of T2.
        members.put("B", member(Set.of(T1, T2, T3), new Assignment(Map.of(T1, Set.of(2), T2, Set.of(2, 3)))));
        // C holds one partition of T3, more than its base of zero.
        members.put("C", member(Set.of(T1, T2, T3), new Assignment(Map.of(T3, Set.of(0)))));
        members.put("D", member(Set.of(T1, T2, T3), Assignment.EMPTY));

        GroupModel model = new GroupModel(spec(members), describer(5, 4, 1), false);

        assertTrue(model.isBacked(0, 0));
        assertTrue(model.isBacked(0, 1));
        assertFalse(model.isBacked(0, 2));
        assertFalse(model.isBacked(1, 0));
        assertTrue(model.isBacked(1, 1));
        assertFalse(model.isBacked(1, 2));
        assertFalse(model.isBacked(2, 0));
        assertFalse(model.isBacked(2, 1));
        assertTrue(model.isBacked(2, 2));
        for (int t = 0; t < 3; t++) {
            assertFalse(model.isBacked(3, t));
        }
        assertEquals(2, model.backedCount(0));
        assertEquals(1, model.backedCount(1));
        assertEquals(1, model.backedCount(2));
        assertEquals(0, model.backedCount(3));
    }

    @Test
    public void testBackedTopicsBeyondOneWordOfTheBitset() {
        // Thirty topics with 4 partitions each for three members: base 1, so a member holding two
        // partitions of a topic is backed for it. The 90 member and topic pairs are more than a
        // word of the bitset holds: with the topic first and three members, topic 21 of A is pair
        // 63, the last of the first word, and topic 21 of B is pair 64, the first of the second.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Uuid[] topicIds = new Uuid[30];
        Set<Uuid> topics = new HashSet<>();
        for (int t = 0; t < 30; t++) {
            topicIds[t] = new Uuid(1L, 1L + t);
            builder.addTopic(topicIds[t], "topic-" + t, 4, 3, 1);
            topics.add(topicIds[t]);
        }
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(topics, new Assignment(Map.of(topicIds[0], Set.of(0, 1), topicIds[21], Set.of(0, 1)))));
        members.put("B", member(topics, new Assignment(Map.of(topicIds[20], Set.of(0, 1), topicIds[21], Set.of(2, 3)))));
        // C holds a single partition of topic 22, its base, so it is only backed for topic 29.
        members.put("C", member(topics, new Assignment(Map.of(topicIds[22], Set.of(0), topicIds[29], Set.of(0, 1)))));
        GroupModel model = new GroupModel(spec(members), builder.buildDescriber(), false);
        assertEquals(30, model.topicCount());

        Set<Integer> backed = Set.of(0, 21, 100 + 20, 100 + 21, 200 + 29);
        for (int m = 0; m < model.memberCount(); m++) {
            for (int t = 0; t < model.topicCount(); t++) {
                assertEquals(backed.contains(m * 100 + t), model.isBacked(m, t), "member " + m + " topic " + t);
            }
        }
        assertEquals(2, model.backedCount(0));
        assertEquals(2, model.backedCount(1));
        assertEquals(1, model.backedCount(2));
    }

    @Test
    public void testRacksAreNotUsedWhenDisabled() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member("r2", Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, rackDescriber(), false);

        assertFalse(model.usesRacks());
        assertEquals(0, model.racks().count());
        assertNull(model.racks().memberRack());
        assertNull(model.racks().partitionRacks());
        assertNull(model.racks().supply());
        assertEquals(1, model.cohorts().count());
        assertArrayEquals(new int[]{0, 0}, model.cohorts().memberCohort());
    }

    @Test
    public void testRacksAreNotUsedWhenAMemberHasNoRack() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member("r2", Set.of(T1, T2), Assignment.EMPTY));
        members.put("C", member(Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, rackDescriber(), true);

        assertFalse(model.usesRacks());
        assertEquals(0, model.racks().count());
        assertEquals(1, model.cohorts().count());
    }

    @Test
    public void testRacksAreNotUsedWhenAllMembersShareOneRack() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member("r1", Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, rackDescriber(), true);

        assertFalse(model.usesRacks());
        assertEquals(0, model.racks().count());
        assertEquals(1, model.cohorts().count());
    }

    @Test
    public void testRacksAreUsedForAtMost64Racks() {
        assertTrue(modelWithOneMemberPerRack(64).usesRacks());
        assertEquals(64, modelWithOneMemberPerRack(64).racks().count());
        assertFalse(modelWithOneMemberPerRack(65).usesRacks());
        assertEquals(0, modelWithOneMemberPerRack(65).racks().count());
    }

    private static GroupModel modelWithOneMemberPerRack(int rackCount) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < rackCount; i++) {
            members.put(String.format("member-%03d", i), member("rack-" + i, Set.of(T1), Assignment.EMPTY));
        }
        return model(members, describer(rackCount), true);
    }

    @Test
    public void testRacks() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1, T2), Assignment.EMPTY));
        members.put("B", member("r2", Set.of(T1, T2), Assignment.EMPTY));
        members.put("C", member("r1", Set.of(T1, T2), Assignment.EMPTY));

        GroupModel model = model(members, rackDescriber(), true);

        assertTrue(model.usesRacks());
        // Racks are numbered in the order of their first member: r1 is rack 0 and r2 is rack 1.
        // Rack r3 has no member and is not counted.
        assertEquals(2, model.racks().count());
        assertArrayEquals(new int[]{0, 1, 0}, model.racks().memberRack());
        // One bit per rack: partition 2 of T1 has replicas in both racks and partition 3 only in r3.
        assertArrayEquals(new long[]{0b01, 0b10, 0b11, 0b00}, model.racks().partitionRacks()[0]);
        assertArrayEquals(new long[]{0b01, 0b10}, model.racks().partitionRacks()[1]);
        // Per topic and rack, the number of partitions with a replica in the rack.
        assertArrayEquals(new int[]{2, 2}, model.racks().supply()[0]);
        assertArrayEquals(new int[]{1, 1}, model.racks().supply()[1]);

        // Homogeneous with racks: one cohort per rack, the cohort of a rack having the index of the rack.
        assertEquals(2, model.cohorts().count());
        assertArrayEquals(new int[]{0, 1, 0}, model.cohorts().memberCohort());
        assertArrayEquals(new int[]{0, 1}, model.cohorts().rack());
        assertArrayEquals(new int[]{2, 1}, model.cohorts().size());
        // T1: 4 / 3 = 1 base partition, T2: 2 / 3 = 0, so a base load of 1 for both cohorts.
        assertArrayEquals(new int[]{1, 1}, model.cohorts().baseLoad());
        assertArrayEquals(new int[]{0, 1}, model.cohorts().topics()[0]);
        assertArrayEquals(new int[]{0, 1}, model.cohorts().topics()[1]);
        assertArrayEquals(new int[]{0, 2, 4}, model.cohorts().topicCohortStart());
        assertArrayEquals(new int[]{0, 1, 0, 1}, model.cohorts().topicCohorts());
        assertEquals(2, model.maxCohortsPerTopic());
    }

    @Test
    public void testHeterogeneousCohortsAreSplitByRack() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member("r1", Set.of(T1), Assignment.EMPTY));
        members.put("B", member("r2", Set.of(T1), Assignment.EMPTY));
        members.put("C", member("r1", Set.of(T1), Assignment.EMPTY));
        members.put("D", member("r1", Set.of(T1, T2), Assignment.EMPTY));

        // With racks, a cohort has one subscription and one rack: {A, C}, {B} and {D}.
        GroupModel model = model(members, rackDescriber(), true);
        assertTrue(model.usesRacks());
        assertEquals(3, model.cohorts().count());
        assertArrayEquals(new int[]{0, 1, 0, 2}, model.cohorts().memberCohort());
        assertArrayEquals(new int[]{0, 1, 0}, model.cohorts().rack());
        assertArrayEquals(new int[]{2, 1, 1}, model.cohorts().size());
        assertArrayEquals(new int[]{0}, model.cohorts().topics()[0]);
        assertArrayEquals(new int[]{0}, model.cohorts().topics()[1]);
        assertArrayEquals(new int[]{0, 1}, model.cohorts().topics()[2]);

        // Without racks, A, B and C form a single cohort.
        GroupModel plain = model(members, rackDescriber(), false);
        assertEquals(2, plain.cohorts().count());
        assertArrayEquals(new int[]{0, 0, 0, 1}, plain.cohorts().memberCohort());
        assertArrayEquals(new int[]{0, 0}, plain.cohorts().rack());
    }

    /**
     * Current partition ids beyond the partition count of the topic exist in the input but are
     * not counted: they neither make the owner backed nor count as a stale entry.
     */
    @Test
    public void testCurrentPartitionsBeyondThePartitionCountAreNotCounted() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(T1), new Assignment(Map.of(T1, Set.of(0, 5)))));
        members.put("B", member(Set.of(T1), new Assignment(Map.of(T1, Set.of(1)))));
        members.put("C", member(Set.of(T1), Assignment.EMPTY));

        // T1 has 3 partitions for 3 subscribers, so the base is 1.
        GroupModel model = model(members, describer(3), false);

        assertArrayEquals(new int[]{0, 2}, model.owners().start());
        assertArrayEquals(new int[]{0, 1}, model.owners().member());
        assertEquals(Set.of(0, 5), model.owners().partitions()[0]);
        assertArrayEquals(new int[]{1, 1}, model.owners().validCount());
        assertFalse(model.isBacked(0, 0));
        assertEquals(0, model.backedCount(0));
        assertFalse(model.hasStalePartitions()[0]);
        assertEquals(0, model.backedCount(1));
        assertEquals(0, model.backedCount(2));
    }
}
