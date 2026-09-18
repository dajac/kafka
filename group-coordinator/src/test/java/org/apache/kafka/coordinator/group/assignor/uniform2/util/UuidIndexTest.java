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
package org.apache.kafka.coordinator.group.assignor.uniform2.util;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UuidIndexTest {
    private static final Uuid TOPIC_1 = new Uuid(1L, 1L);
    private static final Uuid TOPIC_2 = new Uuid(1L, 2L);
    private static final Uuid TOPIC_3 = new Uuid(1L, 3L);

    @Test
    public void testNoTopics() {
        var index = new UuidIndex(new Uuid[0]);
        assertEquals(UuidIndex.NONE, index.indexOf(TOPIC_1));
        assertEquals(UuidIndex.NONE, index.indexOf(Uuid.ZERO_UUID));
    }

    @Test
    public void testSingleTopic() {
        var index = new UuidIndex(new Uuid[] {TOPIC_1});
        assertEquals(0, index.indexOf(TOPIC_1));
        assertEquals(UuidIndex.NONE, index.indexOf(TOPIC_2));
        // Ids sharing one of the two longs with the topic are still unknown.
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(1L, 5L)));
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(5L, 1L)));
    }

    @Test
    public void testTwoTopics() {
        var index = new UuidIndex(new Uuid[] {TOPIC_1, TOPIC_2});
        assertEquals(0, index.indexOf(TOPIC_1));
        assertEquals(1, index.indexOf(TOPIC_2));
        assertEquals(UuidIndex.NONE, index.indexOf(TOPIC_3));
    }

    @Test
    public void testIndexIsThePositionInTheInput() {
        var index = new UuidIndex(new Uuid[] {TOPIC_3, TOPIC_1, TOPIC_2});
        assertEquals(0, index.indexOf(TOPIC_3));
        assertEquals(1, index.indexOf(TOPIC_1));
        assertEquals(2, index.indexOf(TOPIC_2));
    }

    @Test
    public void testLookupWithAnEqualInstance() {
        var index = new UuidIndex(new Uuid[] {TOPIC_1, TOPIC_2});
        assertEquals(0, index.indexOf(new Uuid(1L, 1L)));
        assertEquals(1, index.indexOf(new Uuid(1L, 2L)));
    }

    @Test
    public void testManyTopics() {
        var topics = new Uuid[300];
        for (int i = 0; i < topics.length; i++) {
            topics[i] = new Uuid(1000L + i, 7L * i);
        }
        var index = new UuidIndex(topics);
        for (int i = 0; i < topics.length; i++) {
            assertEquals(i, index.indexOf(topics[i]));
            assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(1000L + i, 7L * i + 1)));
        }
    }

    @Test
    public void testTopicsWithCollidingSlots() {
        // The slot of an id is derived from the xor of its two longs, so these ids all hash to
        // the same slot and are only told apart by linear probing.
        var topics = new Uuid[64];
        for (int i = 0; i < topics.length; i++) {
            topics[i] = new Uuid(i, i ^ 7L);
        }
        var index = new UuidIndex(topics);
        for (int i = 0; i < topics.length; i++) {
            assertEquals(i, index.indexOf(topics[i]));
        }
        // An unknown id hashing to the same slot probes past every colliding id.
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(100L, 100L ^ 7L)));
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(0L, 1L)));
    }

    @Test
    public void testMixOfCollidingAndDistinctSlots() {
        var topics = new Uuid[300];
        for (int i = 0; i < 100; i++) {
            topics[i] = new Uuid(i, i ^ 7L);
            topics[100 + i] = new Uuid(i, i ^ 13L);
            topics[200 + i] = new Uuid(500L + i, 31L * i);
        }
        var index = new UuidIndex(topics);
        for (int i = 0; i < topics.length; i++) {
            assertEquals(i, index.indexOf(topics[i]));
        }
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(100L, 100L ^ 7L)));
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(100L, 100L ^ 13L)));
        assertEquals(UuidIndex.NONE, index.indexOf(new Uuid(600L, 0L)));
    }
}
