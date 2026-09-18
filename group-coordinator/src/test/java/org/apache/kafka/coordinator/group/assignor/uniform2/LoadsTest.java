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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.member;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadsTest {
    private static final Uuid TOPIC_1 = new Uuid(1L, 1L);
    private static final Uuid TOPIC_2 = new Uuid(1L, 2L);
    // Members are numbered in id order.
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int D = 3;
    private static final int E = 4;

    /**
     * Topic 1 and topic 2 with the given partition counts.
     */
    private static SubscribedTopicDescriber describer(int topic1Partitions, int topic2Partitions) {
        TestMetadataImageBuilder builder = new TestMetadataImageBuilder();
        if (topic1Partitions > 0) {
            builder.addTopic(TOPIC_1, "topic-1", topic1Partitions, 4, 2);
        }
        if (topic2Partitions > 0) {
            builder.addTopic(TOPIC_2, "topic-2", topic2Partitions, 4, 2);
        }
        return builder.buildDescriber();
    }

    private static String memberId(int member) {
        return String.valueOf((char) ('A' + member));
    }

    /**
     * @return A group with a single cohort: members A, B, ... all subscribed to both topics.
     */
    private static GroupModel homogeneousModel(int memberCount, int topic1Partitions, int topic2Partitions) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int m = 0; m < memberCount; m++) {
            members.put(memberId(m), member(Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        }
        return new GroupModel(spec(members), describer(topic1Partitions, topic2Partitions));
    }

    /**
     * @return A group with three cohorts: A and B subscribed to both topics, C and D to topic 1
     *         only and E to topic 2 only. Topic 1 has 8 partitions for 4 subscribers and topic 2
     *         has 6 partitions for 3 subscribers, so the base loads are 4, 2 and 2.
     */
    private static GroupModel heterogeneousModel() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put("A", member(Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        members.put("B", member(Set.of(TOPIC_1, TOPIC_2), Assignment.EMPTY));
        members.put("C", member(Set.of(TOPIC_1), Assignment.EMPTY));
        members.put("D", member(Set.of(TOPIC_1), Assignment.EMPTY));
        members.put("E", member(Set.of(TOPIC_2), Assignment.EMPTY));
        return new GroupModel(spec(members), describer(8, 6));
    }

    @Test
    public void testOrderListsMembersByAscendingLoad() {
        // Two topics with 6 partitions each for 3 members: base load 4.
        GroupModel model = homogeneousModel(3, 6, 6);
        assertEquals(1, model.cohorts().count());
        assertEquals(4, model.cohorts().baseLoad()[0]);

        int[] load = {6, 4, 5};
        Loads loads = new Loads(model, load);
        assertSame(load, loads.load);
        assertArrayEquals(new int[] {B, C, A}, loads.order(0));
        assertEquals(4, loads.min());
        assertSorted(model, loads);
    }

    @Test
    public void testOrderWithTiesListsEveryMember() {
        // Two topics with 8 partitions each for 4 members: base load 4.
        GroupModel model = homogeneousModel(4, 8, 8);
        Loads loads = new Loads(model, new int[] {5, 4, 5, 4});
        int[] order = loads.order(0);
        assertEquals(4, order.length);
        assertEquals(Set.of(B, D), Set.of(order[0], order[1]));
        assertEquals(Set.of(A, C), Set.of(order[2], order[3]));
        assertEquals(4, loads.min());
        assertSorted(model, loads);
    }

    @Test
    public void testIncrementAndDecrementKeepTheOrderSorted() {
        // Two topics with 6 partitions each for 3 members: base load 4, at most one extra
        // partition per topic, so loads stay between 4 and 6.
        GroupModel model = homogeneousModel(3, 6, 6);
        int[] load = {4, 4, 4};
        Loads loads = new Loads(model, load);
        int[] order = loads.order(0);
        assertArrayEquals(new int[] {A, B, C}, order);

        loads.increment(A);
        assertArrayEquals(new int[] {5, 4, 4}, load);
        assertEquals(A, order[2]);
        assertEquals(4, loads.min());
        assertSorted(model, loads);

        loads.increment(B);
        assertArrayEquals(new int[] {5, 5, 4}, load);
        assertEquals(C, order[0]);
        assertEquals(4, loads.min());
        assertSorted(model, loads);

        loads.increment(C);
        loads.increment(C);
        assertArrayEquals(new int[] {5, 5, 6}, load);
        assertEquals(C, order[2]);
        assertEquals(5, loads.min());
        assertSorted(model, loads);

        loads.decrement(A);
        assertArrayEquals(new int[] {4, 5, 6}, load);
        assertArrayEquals(new int[] {A, B, C}, order);
        assertEquals(4, loads.min());

        loads.decrement(C);
        loads.decrement(C);
        assertArrayEquals(new int[] {4, 5, 4}, load);
        assertEquals(B, order[2]);
        assertEquals(4, loads.min());
        assertSorted(model, loads);

        loads.decrement(B);
        assertArrayEquals(new int[] {4, 4, 4}, load);
        assertEquals(4, loads.min());
        assertSorted(model, loads);

        // The order is live: the same array reflects every change.
        assertSame(order, loads.order(0));
    }

    @Test
    public void testMultipleCohorts() {
        GroupModel model = heterogeneousModel();
        assertEquals(3, model.cohorts().count());
        assertArrayEquals(new int[] {0, 0, 1, 1, 2}, model.cohorts().memberCohort());
        assertArrayEquals(new int[] {4, 2, 2}, model.cohorts().baseLoad());

        int[] load = {6, 5, 2, 3, 2};
        Loads loads = new Loads(model, load);
        assertArrayEquals(new int[] {B, A}, loads.order(0));
        assertArrayEquals(new int[] {C, D}, loads.order(1));
        assertArrayEquals(new int[] {E}, loads.order(2));
        assertEquals(2, loads.min());
        assertSorted(model, loads);

        // A 6, B 5, C 3, D 3, E 2.
        loads.increment(C);
        assertEquals(2, loads.min());
        assertSorted(model, loads);

        // A 6, B 5, C 3, D 3, E 3.
        loads.increment(E);
        assertArrayEquals(new int[] {6, 5, 3, 3, 3}, load);
        assertEquals(3, loads.min());
        assertSorted(model, loads);

        // A 6, B 4, C 3, D 3, E 3.
        loads.decrement(B);
        assertArrayEquals(new int[] {B, A}, loads.order(0));
        assertEquals(3, loads.min());
        assertSorted(model, loads);

        // A 6, B 4, C 3, D 2, E 3.
        loads.decrement(D);
        assertArrayEquals(new int[] {D, C}, loads.order(1));
        assertEquals(2, loads.min());
        assertSorted(model, loads);

        // A 4, B 4, C 3, D 2, E 3.
        loads.decrement(A);
        loads.decrement(A);
        assertArrayEquals(new int[] {4, 4, 3, 2, 3}, load);
        assertEquals(2, loads.min());
        assertSorted(model, loads);

        // A 4, B 5, C 3, D 2, E 3.
        loads.increment(B);
        assertArrayEquals(new int[] {A, B}, loads.order(0));
        assertEquals(2, loads.min());
        assertSorted(model, loads);
    }

    @Test
    public void testRandomIncrementsAndDecrementsKeepEveryCohortSorted() {
        // Twelve members with three different subscriptions: three cohorts.
        List<Set<Uuid>> subscriptions = List.of(Set.of(TOPIC_1, TOPIC_2), Set.of(TOPIC_1), Set.of(TOPIC_2));
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int m = 0; m < 12; m++) {
            members.put(memberId(m), member(subscriptions.get(m % 3), Assignment.EMPTY));
        }
        GroupModel model = new GroupModel(spec(members), describer(10, 9));
        assertEquals(3, model.cohorts().count());

        int[] load = new int[model.memberCount()];
        for (int m = 0; m < model.memberCount(); m++) {
            load[m] = model.cohorts().baseLoad()[model.cohorts().memberCohort()[m]];
        }
        Loads loads = new Loads(model, load);
        assertSorted(model, loads);

        Random random = new Random(17);
        for (int i = 0; i < 2000; i++) {
            int m = random.nextInt(model.memberCount());
            int c = model.cohorts().memberCohort()[m];
            int base = model.cohorts().baseLoad()[c];
            // A member gets at most one extra partition per topic.
            int max = base + model.cohorts().topics()[c].length;
            int before = load[m];
            boolean canIncrement = before < max;
            boolean canDecrement = before > base;
            if (canIncrement && (!canDecrement || random.nextBoolean())) {
                loads.increment(m);
                assertEquals(before + 1, load[m]);
            } else {
                loads.decrement(m);
                assertEquals(before - 1, load[m]);
            }
            assertSorted(model, loads);
        }
    }

    /**
     * Checks that the order of every cohort lists exactly its members by ascending load, by
     * comparing it with the loads of the cohort sorted, and that the minimum is the smallest
     * load of the group.
     */
    private static void assertSorted(GroupModel model, Loads loads) {
        for (int c = 0; c < model.cohorts().count(); c++) {
            int[] order = loads.order(c);
            assertEquals(model.cohorts().size()[c], order.length, "size of cohort " + c);

            int[] expected = new int[order.length];
            int n = 0;
            for (int m = 0; m < model.memberCount(); m++) {
                if (model.cohorts().memberCohort()[m] == c) {
                    expected[n++] = loads.load[m];
                }
            }
            Arrays.sort(expected);

            Set<Integer> seen = new HashSet<>();
            int[] actual = new int[order.length];
            for (int i = 0; i < order.length; i++) {
                int m = order[i];
                assertEquals(c, model.cohorts().memberCohort()[m], "member " + m + " is listed in cohort " + c);
                assertTrue(seen.add(m), "member " + m + " is listed twice in cohort " + c);
                actual[i] = loads.load[m];
            }
            assertArrayEquals(expected, actual, "cohort " + c + " is not sorted by load: " + Arrays.toString(order));
        }

        int min = Integer.MAX_VALUE;
        for (int m = 0; m < model.memberCount(); m++) {
            min = Math.min(min, loads.load[m]);
        }
        assertEquals(min, loads.min());
    }
}
