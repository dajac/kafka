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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.uniform2.TestMetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.alignedPartitions;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertStable;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.load;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.revocations;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.withAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Uniform2AssignorTest {
    // Topics are processed in id order by the assignor, so fixed ordered ids keep the
    // expected assignments deterministic.
    private static final Uuid TOPIC_1 = new Uuid(1L, 1L);
    private static final Uuid TOPIC_2 = new Uuid(1L, 2L);
    private static final Set<Uuid> BOTH_TOPICS = Set.of(TOPIC_1, TOPIC_2);
    private static final String MEMBER_A = "A";
    private static final String MEMBER_B = "B";
    private static final String MEMBER_C = "C";
    private static final String MEMBER_D = "D";
    private static final String MEMBER_E = "E";

    private final Uniform2Assignor assignor = new Uniform2Assignor();
    private final Uniform2Assignor rackAwareAssignor = new Uniform2Assignor(true);

    @ParameterizedTest
    @CsvSource({"HOMOGENEOUS, false", "HOMOGENEOUS, true", "HETEROGENEOUS, false", "HETEROGENEOUS, true"})
    public void testAssignmentReuse(SubscriptionType subscriptionType, boolean rackAware) {
        CommonAssignorTests.testAssignmentReuse(rackAware ? rackAwareAssignor : assignor, subscriptionType, rackAware);
    }

    @ParameterizedTest
    @CsvSource({"HOMOGENEOUS, false", "HOMOGENEOUS, true", "HETEROGENEOUS, false", "HETEROGENEOUS, true"})
    public void testReassignmentStickiness(SubscriptionType subscriptionType, boolean rackAware) {
        CommonAssignorTests.testReassignmentStickiness(rackAware ? rackAwareAssignor : assignor, subscriptionType, rackAware);
    }

    @Test
    public void testNameAndConfiguration() {
        assertEquals("uniform2", assignor.name());
        assertFalse(assignor.rackAwareEnabled());
        assertTrue(rackAwareAssignor.rackAwareEnabled());

        Uniform2Assignor configured = new Uniform2Assignor();
        configured.configure(Map.of(Uniform2Assignor.RACK_AWARE_ENABLE_CONFIG, Boolean.TRUE));
        assertTrue(configured.rackAwareEnabled());
        configured.configure(Map.of(Uniform2Assignor.RACK_AWARE_ENABLE_CONFIG, Boolean.FALSE));
        assertFalse(configured.rackAwareEnabled());
        configured.configure(Map.of(Uniform2Assignor.RACK_AWARE_ENABLE_CONFIG, "true"));
        assertTrue(configured.rackAwareEnabled());
        configured.configure(Map.of(Uniform2Assignor.RACK_AWARE_ENABLE_CONFIG, "false"));
        assertFalse(configured.rackAwareEnabled());
        configured.configure(Map.of(Uniform2Assignor.RACK_AWARE_ENABLE_CONFIG, " TRUE "));
        assertTrue(configured.rackAwareEnabled());
        // An absent key resets the default.
        configured.configure(Map.of());
        assertFalse(configured.rackAwareEnabled());
    }

    @Test
    public void testEmptyGroup() {
        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(Map.of(), HOMOGENEOUS, Map.of()),
            new SubscribedTopicDescriberImpl(CoordinatorMetadataImage.EMPTY)
        );
        assertEquals(Map.of(), result.members());
    }

    @Test
    public void testNoSubscribedTopics() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(), Assignment.EMPTY));
        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, Map.of()),
            new SubscribedTopicDescriberImpl(CoordinatorMetadataImage.EMPTY)
        );
        assertEquals(Map.of(), result.members());
    }

    @Test
    public void testSubscribedTopicDoesNotExist() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), Assignment.EMPTY));
        assertThrows(PartitionAssignorException.class, () -> assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, Map.of()),
            new SubscribedTopicDescriberImpl(CoordinatorMetadataImage.EMPTY)
        ));
    }

    @Test
    public void testFirstAssignmentSpreadsEveryTopic() {
        SubscribedTopicDescriber describer = describer(3, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(BOTH_TOPICS, Assignment.EMPTY));
        members.put(MEMBER_B, member(BOTH_TOPICS, Assignment.EMPTY));

        GroupAssignment result = assignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1), mkTopicAssignment(TOPIC_2, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1, 2)));
        assertAssignment(expected, result);
    }

    @Test
    public void testHomogeneousSubscriptionsAreReadThroughTheTopicNames() {
        // The coordinator gives the subscribed topic ids of a member as a view over its
        // subscribed topic names, which resolves the ids while iterating and supports neither
        // toArray nor the other bulk operations. The assignment is the one of the same group
        // subscribed by ids, see testFirstAssignmentSpreadsEveryTopic.
        CoordinatorMetadataImage image = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 3, 4, 2)
            .addTopic(TOPIC_2, "topic-2", 3, 4, 2)
            .buildImage();
        Set<Uuid> subscription = new TopicIds(Set.of("topic-1", "topic-2"), image);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(subscription, Assignment.EMPTY));
        members.put(MEMBER_B, member(subscription, Assignment.EMPTY));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, Map.of()),
            new SubscribedTopicDescriberImpl(image)
        );

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1), mkTopicAssignment(TOPIC_2, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1, 2)));
        assertAssignment(expected, result);
    }

    @Test
    public void testHeterogeneousSubscriptionsAreReadThroughTheTopicNames() {
        CoordinatorMetadataImage image = new TestMetadataImageBuilder()
            .addTopic(TOPIC_1, "topic-1", 3, 4, 2)
            .addTopic(TOPIC_2, "topic-2", 3, 4, 2)
            .buildImage();
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(new TopicIds(Set.of("topic-1", "topic-2"), image), Assignment.EMPTY));
        members.put(MEMBER_B, member(new TopicIds(Set.of("topic-2"), image), Assignment.EMPTY));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HETEROGENEOUS, Map.of()),
            new SubscribedTopicDescriberImpl(image)
        );

        // A is the only subscriber of topic 1 and takes its three partitions, so the extra
        // partition of topic 2 goes to B, which has the lower load.
        MemberAssignment a = result.members().get(MEMBER_A);
        MemberAssignment b = result.members().get(MEMBER_B);
        assertEquals(Set.of(0, 1, 2), a.partitions().get(TOPIC_1));
        assertEquals(1, a.partitions().get(TOPIC_2).size());
        assertFalse(b.partitions().containsKey(TOPIC_1));
        assertEquals(2, b.partitions().get(TOPIC_2).size());
    }

    @Test
    public void testMemberJoinsOnlyMovesItsAllocation() {
        SubscribedTopicDescriber describer = describer(3, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 0, 1), mkTopicAssignment(TOPIC_2, 0)))));
        members.put(MEMBER_B, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1, 2)))));
        members.put(MEMBER_C, member(BOTH_TOPICS, Assignment.EMPTY));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, invertedTargetAssignment(members)), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0), mkTopicAssignment(TOPIC_2, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1)));
        expected.put(MEMBER_C, mkAssignment(mkTopicAssignment(TOPIC_1, 1), mkTopicAssignment(TOPIC_2, 2)));
        assertAssignment(expected, result);
        assertEquals(2, revocations(members, result));
    }

    @Test
    public void testMemberLeavesWithoutRevocations() {
        SubscribedTopicDescriber describer = describer(3, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 0), mkTopicAssignment(TOPIC_2, 0)))));
        members.put(MEMBER_B, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1)))));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, invertedTargetAssignment(members)), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1), mkTopicAssignment(TOPIC_2, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 1, 2)));
        assertAssignment(expected, result);
        assertEquals(0, revocations(members, result));
    }

    @Test
    public void testPartitionsAddedToTopic() {
        SubscribedTopicDescriber describer = describer(5, 0);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1)))));
        members.put(MEMBER_B, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 2)))));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, invertedTargetAssignment(members)), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1, 3)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2, 4)));
        assertAssignment(expected, result);
        assertEquals(0, revocations(members, result));
    }

    @Test
    public void testUnsubscribedTopicIsStale() {
        SubscribedTopicDescriber describer = describer(2, 2);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 0), mkTopicAssignment(TOPIC_2, 0, 1)))));
        members.put(MEMBER_B, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 1)))));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, invertedTargetAssignment(members)), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 1)));
        assertAssignment(expected, result);
        // B did not change and gets its own map back.
        assertSame(members.get(MEMBER_B).partitions(), result.members().get(MEMBER_B).partitions());
    }

    @Test
    public void testValidAssignmentIsReturnedUnchanged() {
        SubscribedTopicDescriber describer = describer(3, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        // Spread within one per topic and in total, but not the assignment the assignor
        // would compute from scratch.
        members.put(MEMBER_A, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 0, 2), mkTopicAssignment(TOPIC_2, 1)))));
        members.put(MEMBER_B, member(BOTH_TOPICS, new Assignment(mkAssignment(
            mkTopicAssignment(TOPIC_1, 1), mkTopicAssignment(TOPIC_2, 0, 2)))));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, HOMOGENEOUS, invertedTargetAssignment(members)), describer);

        for (String memberId : members.keySet()) {
            assertSame(members.get(memberId).partitions(), result.members().get(memberId).partitions());
        }
    }

    @Test
    public void testHeterogeneousSubscriptionsSpreadEveryTopic() {
        SubscribedTopicDescriber describer = describer(4, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), Assignment.EMPTY));
        members.put(MEMBER_B, member(BOTH_TOPICS, Assignment.EMPTY));

        GroupAssignment result = assignor.assign(new GroupSpecImpl(members, HETEROGENEOUS, Map.of()), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2, 3), mkTopicAssignment(TOPIC_2, 0, 1, 2)));
        assertAssignment(expected, result);
    }

    @Test
    public void testHeterogeneousSubscriptionsBalanceExtraPartitions() {
        SubscribedTopicDescriber describer = describer(3, 3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), Assignment.EMPTY));
        members.put(MEMBER_B, member(BOTH_TOPICS, Assignment.EMPTY));
        members.put(MEMBER_C, member(Set.of(TOPIC_2), Assignment.EMPTY));

        GroupAssignment result = assignor.assign(new GroupSpecImpl(members, HETEROGENEOUS, Map.of()), describer);

        // The extra partition of each topic goes to the member with the lowest load, so B,
        // which subscribes to both topics, ends with the same number of partitions as A and C.
        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 1)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 2), mkTopicAssignment(TOPIC_2, 0)));
        expected.put(MEMBER_C, mkAssignment(mkTopicAssignment(TOPIC_2, 1, 2)));
        assertAssignment(expected, result);
    }

    @Test
    public void testLargeGroupIsValidAndBalanced() {
        int topicCount = 100;
        int memberCount = 49;
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int i = 0; i < topicCount; i++) {
            Uuid topicId = Uuid.randomUuid();
            topics.add(topicId);
            builder.addTopic(topicId, "topic-" + i, 3, 4, 2);
        }
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < memberCount; i++) {
            members.put("member-" + i, member(topics, Assignment.EMPTY));
        }
        GroupAssignment result = assignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), builder.buildDescriber());

        int min = Integer.MAX_VALUE;
        int max = 0;
        Map<Uuid, Set<Integer>> seen = new HashMap<>();
        for (MemberAssignment memberAssignment : result.members().values()) {
            int size = 0;
            for (Map.Entry<Uuid, Set<Integer>> entry : memberAssignment.partitions().entrySet()) {
                // Each topic has 3 partitions for 49 members: at most one per member.
                assertEquals(1, entry.getValue().size());
                size += entry.getValue().size();
                for (int partition : entry.getValue()) {
                    assertTrue(seen.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(partition));
                }
            }
            min = Math.min(min, size);
            max = Math.max(max, size);
        }
        assertEquals(topicCount, seen.size());
        seen.values().forEach(partitions -> assertEquals(3, partitions.size()));
        assertTrue(max - min <= 1, "min=" + min + " max=" + max);
    }

    @Test
    public void testRackAwarenessIsIgnoredWhenDisabled() {
        SubscribedTopicDescriber describer = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> racked = new TreeMap<>();
        Map<String, MemberSubscriptionAndAssignmentImpl> unracked = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            String memberId = "member-" + i;
            racked.put(memberId, member("rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
            unracked.put(memberId, member(Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment withRacks = assignor.assign(new GroupSpecImpl(racked, HOMOGENEOUS, Map.of()), describer);
        GroupAssignment withoutRacks = assignor.assign(new GroupSpecImpl(unracked, HOMOGENEOUS, Map.of()), describer);
        assertEquals(withoutRacks, withRacks);
    }

    @Test
    public void testRackAwarenessRequiresARackForEveryMember() {
        SubscribedTopicDescriber describer = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> partiallyRacked = new TreeMap<>();
        Map<String, MemberSubscriptionAndAssignmentImpl> unracked = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            String memberId = "member-" + i;
            partiallyRacked.put(memberId, member(i == 1 ? null : "rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
            unracked.put(memberId, member(Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment partial = rackAwareAssignor.assign(new GroupSpecImpl(partiallyRacked, HOMOGENEOUS, Map.of()), describer);
        GroupAssignment none = rackAwareAssignor.assign(new GroupSpecImpl(unracked, HOMOGENEOUS, Map.of()), describer);
        assertEquals(none, partial);
    }

    @Test
    public void testRackAwareAssignmentIsFullyAlignedWithTwoReplicasAndThreeRacks() {
        SubscribedTopicDescriber describer = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            members.put("member-" + i, member("rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment result = rackAwareAssignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), describer);

        for (MemberAssignment memberAssignment : result.members().values()) {
            assertEquals(2, memberAssignment.partitions().get(TOPIC_1).size());
        }
        assertEquals(6, alignedPartitions(members, result, describer));
    }

    @Test
    public void testRackAwareAssignmentRealignsAfterReplicasMove() {
        // Partition i initially has replicas on brokers i and i + 1.
        SubscribedTopicDescriber before = rackDescriber(3);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            members.put("member-" + i, member("rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment aligned = rackAwareAssignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), before);
        assertEquals(3, alignedPartitions(members, aligned, before));

        // The replicas of every partition move to the two other brokers.
        SubscribedTopicDescriber after = new TestMetadataImageBuilder()
            .addBroker(0, "rack-0").addBroker(1, "rack-1").addBroker(2, "rack-2")
            .addTopic(TOPIC_1, "topic-1", List.of(List.of(1, 2), List.of(2, 0), List.of(0, 1)))
            .buildDescriber();
        Map<String, MemberSubscriptionAndAssignmentImpl> membersWithAssignment = withAssignment(members, aligned);
        assertEquals(0, alignedPartitions(membersWithAssignment, aligned, after));

        GroupAssignment realigned = rackAwareAssignor.assign(
            new GroupSpecImpl(membersWithAssignment, HOMOGENEOUS, invertedTargetAssignment(membersWithAssignment)), after);
        assertEquals(3, alignedPartitions(membersWithAssignment, realigned, after));
        for (MemberAssignment memberAssignment : realigned.members().values()) {
            assertEquals(1, memberAssignment.partitions().get(TOPIC_1).size());
        }
    }

    @Test
    public void testRackAwareMemberJoinKeepsFullAlignment() {
        SubscribedTopicDescriber describer = rackDescriber(12);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            members.put("member-" + i, member("rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment initial = rackAwareAssignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), describer);
        assertEquals(12, alignedPartitions(members, initial, describer));

        Map<String, MemberSubscriptionAndAssignmentImpl> membersWithAssignment = withAssignment(members, initial);
        membersWithAssignment.put(MEMBER_D, member("rack-1", Set.of(TOPIC_1), Assignment.EMPTY));
        GroupAssignment result = rackAwareAssignor.assign(
            new GroupSpecImpl(membersWithAssignment, HOMOGENEOUS, invertedTargetAssignment(membersWithAssignment)), describer);

        for (MemberAssignment memberAssignment : result.members().values()) {
            assertEquals(3, memberAssignment.partitions().get(TOPIC_1).size());
        }
        assertEquals(12, alignedPartitions(membersWithAssignment, result, describer));
        // D needs three partitions with a replica in rack-1, which are all owned by member-0
        // and member-1. Each of them releases one partition, so the third one requires a swap:
        // member-0 gives a second partition to D and takes the partition released by member-2.
        assertEquals(4, revocations(membersWithAssignment, result));
    }

    @Test
    public void testGroupSpecMemberOrderDoesNotMatter() {
        SubscribedTopicDescriber describer = describer(7, 5);
        List<String> ids = List.of("m1", "m2", "m3", "m4");
        GroupAssignment reference = null;
        for (int rotation = 0; rotation < ids.size(); rotation++) {
            Map<String, MemberSubscriptionAndAssignmentImpl> members = new LinkedHashMap<>();
            for (int i = 0; i < ids.size(); i++) {
                members.put(ids.get((i + rotation) % ids.size()), member(BOTH_TOPICS, Assignment.EMPTY));
            }
            GroupAssignment result = assignor.assign(new GroupSpecImpl(members, HOMOGENEOUS, Map.of()), describer);
            if (reference == null) {
                reference = result;
            } else {
                assertEquals(reference, result);
            }
        }
    }

    @Test
    public void testManyMembersJoinAtOnce() {
        // Two topics with 10 partitions each: A and B hold 5 of each. Three members join: 20
        // partitions for 5 members, 4 each. A and B keep 4 of their 10 and release 6 each,
        // exactly the 12 partitions the joiners are owed.
        SubscribedTopicDescriber describer = describer(10, 10);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(BOTH_TOPICS, Assignment.EMPTY));
        members.put(MEMBER_B, member(BOTH_TOPICS, Assignment.EMPTY));
        GroupAssignment initial = assignor.assign(spec(members), describer);
        assertValidAssignment(members, describer, initial);
        assertEquals(10, load(initial, MEMBER_A));
        assertEquals(10, load(initial, MEMBER_B));

        Map<String, MemberSubscriptionAndAssignmentImpl> grown = withAssignment(members, initial);
        for (String joiner : List.of(MEMBER_C, MEMBER_D, MEMBER_E)) {
            grown.put(joiner, member(BOTH_TOPICS, Assignment.EMPTY));
        }
        GroupAssignment result = assignor.assign(spec(grown), describer);

        assertValidAssignment(grown, describer, result);
        for (String memberId : grown.keySet()) {
            assertEquals(4, load(result, memberId), memberId);
        }
        int intake = load(result, MEMBER_C) + load(result, MEMBER_D) + load(result, MEMBER_E);
        assertEquals(12, intake);
        assertEquals(intake, revocations(grown, result));
        assertStable(grown, describer, result, assignor);
    }

    @Test
    public void testManyMembersLeaveAtOnce() {
        // Five members hold 4 partitions each of two topics with 10 partitions each. Three of
        // them leave: A and B keep everything they hold and share the 12 freed partitions.
        SubscribedTopicDescriber describer = describer(10, 10);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (String memberId : List.of(MEMBER_A, MEMBER_B, MEMBER_C, MEMBER_D, MEMBER_E)) {
            members.put(memberId, member(BOTH_TOPICS, Assignment.EMPTY));
        }
        GroupAssignment initial = assignor.assign(spec(members), describer);
        assertValidAssignment(members, describer, initial);

        Map<String, MemberSubscriptionAndAssignmentImpl> shrunk = withAssignment(members, initial);
        shrunk.keySet().retainAll(Set.of(MEMBER_A, MEMBER_B));
        GroupAssignment result = assignor.assign(spec(shrunk), describer);

        assertValidAssignment(shrunk, describer, result);
        assertEquals(0, revocations(shrunk, result));
        assertEquals(10, load(result, MEMBER_A));
        assertEquals(10, load(result, MEMBER_B));
        assertStable(shrunk, describer, result, assignor);
    }

    @Test
    public void testMembersJoinOneAtATime() {
        // Two topics with 10 and 8 partitions, from one member to six. At every step the
        // assignment is valid, so every topic is spread and the loads are within one, the
        // joiner takes what it is owed and little else moves, and the result is a fixed point.
        SubscribedTopicDescriber describer = describer(10, 8);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("m0", member(BOTH_TOPICS, Assignment.EMPTY));
        GroupAssignment result = assignor.assign(spec(members), describer);
        assertValidAssignment(members, describer, result);
        assertEquals(18, load(result, "m0"));

        for (int i = 1; i < 6; i++) {
            String joiner = "m" + i;
            members = withAssignment(members, result);
            members.put(joiner, member(BOTH_TOPICS, Assignment.EMPTY));
            result = assignor.assign(spec(members), describer);

            String context = "after " + joiner + " joined";
            assertValidAssignment(members, describer, result, context);
            int intake = load(result, joiner);
            assertEquals(18 / (i + 1), intake, context);
            assertTrue(revocations(members, result) <= intake + 1,
                context + ": " + revocations(members, result) + " revocations for an intake of " + intake);
            assertStable(members, describer, result, assignor);
        }
    }

    @Test
    public void testMembersLeaveOneAtATime() {
        // Two topics with 10 and 8 partitions, from six members to one. A leaving member only
        // frees the partitions it held: the others keep everything at every step.
        SubscribedTopicDescriber describer = describer(10, 8);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < 6; i++) {
            members.put("m" + i, member(BOTH_TOPICS, Assignment.EMPTY));
        }
        GroupAssignment result = assignor.assign(spec(members), describer);
        assertValidAssignment(members, describer, result);

        for (int i = 5; i > 0; i--) {
            String leaver = "m" + i;
            members = withAssignment(members, result);
            members.remove(leaver);
            result = assignor.assign(spec(members), describer);

            String context = "after " + leaver + " left";
            assertValidAssignment(members, describer, result, context);
            assertEquals(0, revocations(members, result), context);
            assertStable(members, describer, result, assignor);
        }
        assertEquals(18, load(result, "m0"));
    }

    @Test
    public void testMemberChangesSubscription() {
        // Three members subscribe to two topics with 6 partitions each and hold 2 of each. B
        // drops T2: its two partitions of T2 leave it and go to A and C, which keep theirs. B
        // then subscribes to T2 again and takes one partition back from each of them.
        SubscribedTopicDescriber describer = describer(6, 6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(BOTH_TOPICS, Assignment.EMPTY));
        members.put(MEMBER_B, member(BOTH_TOPICS, Assignment.EMPTY));
        members.put(MEMBER_C, member(BOTH_TOPICS, Assignment.EMPTY));
        GroupAssignment initial = assignor.assign(spec(members), describer);
        assertValidAssignment(members, describer, initial);

        Map<String, MemberSubscriptionAndAssignmentImpl> narrowed = withAssignment(members, initial);
        narrowed.put(MEMBER_B, member(Set.of(TOPIC_1), new Assignment(initial.members().get(MEMBER_B).partitions())));
        assertEquals(HETEROGENEOUS, spec(narrowed).subscriptionType());
        GroupAssignment result = assignor.assign(spec(narrowed), describer);

        assertValidAssignment(narrowed, describer, result);
        assertFalse(result.members().get(MEMBER_B).partitions().containsKey(TOPIC_2));
        assertEquals(2, load(result, MEMBER_B));
        assertEquals(5, load(result, MEMBER_A));
        assertEquals(5, load(result, MEMBER_C));
        // Only the two partitions of T2 that B held move.
        assertEquals(2, revocations(narrowed, result));
        assertStable(narrowed, describer, result, assignor);

        Map<String, MemberSubscriptionAndAssignmentImpl> widened = withAssignment(narrowed, result);
        widened.put(MEMBER_B, member(BOTH_TOPICS, new Assignment(result.members().get(MEMBER_B).partitions())));
        assertEquals(HOMOGENEOUS, spec(widened).subscriptionType());
        GroupAssignment back = assignor.assign(spec(widened), describer);

        assertValidAssignment(widened, describer, back);
        for (String memberId : widened.keySet()) {
            assertEquals(4, load(back, memberId), memberId);
        }
        assertEquals(2, back.members().get(MEMBER_B).partitions().get(TOPIC_2).size());
        assertEquals(2, revocations(widened, back));
        assertStable(widened, describer, back, assignor);
    }

    @Test
    public void testMemberChangesRack() {
        // Six partitions for three members in three racks, fully aligned: 0 and 3 have a replica
        // in rack-0, 1 and 4 in rack-1, 2 and 5 in rack-2. C moves from rack-2 to rack-1: its
        // partitions 2 and 5 only have replicas in rack-2 and rack-0, so they are misaligned
        // and no member of rack-1 is below its allocation. They are swapped with the partitions of
        // A, the member of rack-0, which have a replica in rack-1: C takes 0 and 3, A takes 2
        // and 5, and the allocations do not change. B is untouched.
        SubscribedTopicDescriber describer = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member("rack-0", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 0, 3)))));
        members.put(MEMBER_B, member("rack-1", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 1, 4)))));
        members.put(MEMBER_C, member("rack-1", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 2, 5)))));

        GroupAssignment result = rackAwareAssignor.assign(spec(members), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 2, 5)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 1, 4)));
        expected.put(MEMBER_C, mkAssignment(mkTopicAssignment(TOPIC_1, 0, 3)));
        assertAssignment(expected, result);
        assertEquals(6, alignedPartitions(members, result, describer));
        assertEquals(4, revocations(members, result));
        assertSame(members.get(MEMBER_B).partitions(), result.members().get(MEMBER_B).partitions());
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, rackAwareAssignor);
    }

    @Test
    public void testReplicasMoveRealignWithoutChangingAllocations() {
        // Six partitions for three members in three racks, fully aligned. The replicas of every
        // partition then move to the two other brokers, so that every partition is misaligned.
        // Every member keeps a allocation of 2 and gets two aligned partitions instead.
        SubscribedTopicDescriber before = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member("rack-0", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 0, 3)))));
        members.put(MEMBER_B, member("rack-1", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 1, 4)))));
        members.put(MEMBER_C, member("rack-2", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 2, 5)))));
        GroupAssignment unchanged = rackAwareAssignor.assign(spec(members), before);
        assertEquals(6, alignedPartitions(members, unchanged, before));
        assertEquals(0, revocations(members, unchanged));

        // Partition i now has replicas on brokers i + 1 and i + 2.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder()
            .addBroker(0, "rack-0").addBroker(1, "rack-1").addBroker(2, "rack-2");
        List<List<Integer>> replicas = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            replicas.add(List.of((i + 1) % 3, (i + 2) % 3));
        }
        SubscribedTopicDescriber after = builder.addTopic(TOPIC_1, "topic-1", replicas).buildDescriber();
        assertEquals(0, alignedPartitions(members, unchanged, after));

        GroupAssignment result = rackAwareAssignor.assign(spec(members), after);

        for (String memberId : members.keySet()) {
            assertEquals(2, load(result, memberId), memberId);
        }
        assertEquals(6, alignedPartitions(members, result, after));
        assertEquals(6, revocations(members, result));
        assertValidAssignment(members, after, result);
        assertStable(members, after, result, rackAwareAssignor);
    }

    @Test
    public void testRackAwarenessFallsBackWithMoreThan64Racks() {
        // 65 members in 65 distinct racks: rack awareness cannot be used and the result is the
        // one of the plain algorithm.
        SubscribedTopicDescriber describer = rackDescriber(130);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 0; i < 65; i++) {
            members.put(String.format("member-%02d", i), member("rack-" + i, Set.of(TOPIC_1), Assignment.EMPTY));
        }
        GroupAssignment rackAware = rackAwareAssignor.assign(spec(members), describer);
        GroupAssignment plain = assignor.assign(spec(members), describer);
        assertEquals(plain, rackAware);
        for (String memberId : members.keySet()) {
            assertEquals(2, load(rackAware, memberId), memberId);
        }
        assertValidAssignment(members, describer, rackAware);
    }

    @Test
    public void testRackAwarenessFallsBackWithASingleRack() {
        // All members are in rack-0: nothing can be aligned differently, so the misaligned but
        // settled assignment is returned as is, like the plain algorithm does.
        SubscribedTopicDescriber describer = rackDescriber(6);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member("rack-0", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 1, 4)))));
        members.put(MEMBER_B, member("rack-0", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 2, 5)))));
        members.put(MEMBER_C, member("rack-0", Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 0, 3)))));

        GroupAssignment rackAware = rackAwareAssignor.assign(spec(members), describer);
        GroupAssignment plain = assignor.assign(spec(members), describer);

        assertEquals(plain, rackAware);
        for (String memberId : members.keySet()) {
            assertSame(members.get(memberId).partitions(), rackAware.members().get(memberId).partitions());
        }
    }

    @Test
    public void testFewerPartitionsThanMembers() {
        // One topic with 2 partitions for 4 members: two members get one partition, the others
        // get nothing but are still part of the assignment.
        SubscribedTopicDescriber describer = describer(2, 0);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (String memberId : List.of(MEMBER_A, MEMBER_B, MEMBER_C, MEMBER_D)) {
            members.put(memberId, member(Set.of(TOPIC_1), Assignment.EMPTY));
        }

        GroupAssignment result = assignor.assign(spec(members), describer);

        assertEquals(members.keySet(), result.members().keySet());
        int total = 0;
        int empty = 0;
        for (String memberId : members.keySet()) {
            int memberLoad = load(result, memberId);
            assertTrue(memberLoad <= 1, memberId);
            total += memberLoad;
            if (memberLoad == 0) {
                assertTrue(result.members().get(memberId).partitions().isEmpty());
                empty++;
            }
        }
        assertEquals(2, total);
        assertEquals(2, empty);
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testSinglePartitionTopicsOnly() {
        // Five topics with one partition each for three members: a base of zero everywhere and
        // one extra partition per topic, so two members get 2 partitions and one gets 1.
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        Set<Uuid> topics = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            Uuid topicId = new Uuid(1L, i);
            topics.add(topicId);
            builder.addTopic(topicId, "topic-" + i, 1, 4, 2);
        }
        SubscribedTopicDescriber describer = builder.buildDescriber();
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (String memberId : List.of(MEMBER_A, MEMBER_B, MEMBER_C)) {
            members.put(memberId, member(topics, Assignment.EMPTY));
        }

        GroupAssignment result = assignor.assign(spec(members), describer);

        assertValidAssignment(members, describer, result);
        int total = 0;
        for (String memberId : members.keySet()) {
            int memberLoad = load(result, memberId);
            assertTrue(memberLoad == 1 || memberLoad == 2, memberId + " has " + memberLoad);
            total += memberLoad;
        }
        assertEquals(5, total);
        assertStable(members, describer, result, assignor);
    }

    @Test
    public void testOutOfRangeCurrentPartitionsAreStale() {
        // T1 has 3 partitions for 3 members. A holds 0 and 7, which does not exist: A keeps 0
        // and 7 disappears, while B and C are unchanged.
        SubscribedTopicDescriber describer = describer(3, 0);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 0, 7)))));
        members.put(MEMBER_B, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 1)))));
        members.put(MEMBER_C, member(Set.of(TOPIC_1), new Assignment(mkAssignment(mkTopicAssignment(TOPIC_1, 2)))));

        GroupAssignment result = assignor.assign(spec(members), describer);

        Map<String, Map<Uuid, Set<Integer>>> expected = new HashMap<>();
        expected.put(MEMBER_A, mkAssignment(mkTopicAssignment(TOPIC_1, 0)));
        expected.put(MEMBER_B, mkAssignment(mkTopicAssignment(TOPIC_1, 1)));
        expected.put(MEMBER_C, mkAssignment(mkTopicAssignment(TOPIC_1, 2)));
        assertAssignment(expected, result);
        assertSame(members.get(MEMBER_B).partitions(), result.members().get(MEMBER_B).partitions());
        assertSame(members.get(MEMBER_C).partitions(), result.members().get(MEMBER_C).partitions());
        assertValidAssignment(members, describer, result);
        assertStable(members, describer, result, assignor);
    }

    /**
     * Topics 1 and 2 with the given partition counts (0 means absent), without rack info.
     */
    private static SubscribedTopicDescriber describer(int topic1Partitions, int topic2Partitions) {
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        if (topic1Partitions > 0) builder.addTopic(TOPIC_1, "topic-1", topic1Partitions, 4, 2);
        if (topic2Partitions > 0) builder.addTopic(TOPIC_2, "topic-2", topic2Partitions, 4, 2);
        return builder.buildDescriber();
    }

    /**
     * Topic 1 with the given number of partitions on 3 brokers in 3 racks with 2 replicas:
     * partition i has replicas in rack-(i % 3) and rack-((i + 1) % 3).
     */
    private static SubscribedTopicDescriber rackDescriber(int partitions) {
        return new TestMetadataImageBuilder()
            .addBroker(0, "rack-0").addBroker(1, "rack-1").addBroker(2, "rack-2")
            .addTopic(TOPIC_1, "topic-1", partitions, 3, 2)
            .buildDescriber();
    }
}
