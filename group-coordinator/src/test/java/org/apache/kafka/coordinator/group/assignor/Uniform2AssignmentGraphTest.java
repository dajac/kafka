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

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Uniform2AssignmentGraphTest {
    @Test
    void indexedHeapMatchesOrderedSetUnderDecreasesAndReuse() {
        long[] keys = new long[4096];
        Uniform2AssignmentGraph.NodeHeap heap = new Uniform2AssignmentGraph.NodeHeap(keys);
        TreeSet<Integer> expected = new TreeSet<>(Comparator.<Integer>comparingLong(node -> keys[node]).thenComparingInt(node -> node));
        Random random = new Random(182);
        for (int step = 0; step < 100000; step++) {
            if (step % 20000 == 0) {
                heap.clear();
                expected.clear();
            }
            if (random.nextBoolean() && !expected.isEmpty()) {
                assertEquals(expected.pollFirst().intValue(), heap.remove());
            } else {
                int node = random.nextInt(keys.length);
                expected.remove(node);
                keys[node] -= random.nextInt(1000);
                expected.add(node);
                heap.addOrDecrease(node);
            }
            assertEquals(expected.isEmpty(), heap.isEmpty());
        }
        while (!expected.isEmpty()) assertEquals(expected.pollFirst().intValue(), heap.remove());
        assertTrue(heap.isEmpty());
    }

    @Test
    void heapBreaksEqualKeysByNodeId() {
        Uniform2AssignmentGraph.NodeHeap heap = new Uniform2AssignmentGraph.NodeHeap(new long[10000]);
        for (int node = 9999; node >= 0; node--) heap.addOrDecrease(node);
        for (int node = 0; node < 10000; node++) assertEquals(node, heap.remove());
        assertTrue(heap.isEmpty());
    }
}
