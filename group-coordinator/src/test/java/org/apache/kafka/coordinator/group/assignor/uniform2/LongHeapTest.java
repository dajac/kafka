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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LongHeapTest {

    @Test
    public void testNewHeapIsEmpty() {
        LongHeap heap = new LongHeap(4);
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
    }

    @Test
    public void testPopReturnsValuesInAscendingOrder() {
        LongHeap heap = new LongHeap(8);
        heap.push(5);
        heap.push(1);
        heap.push(4);
        heap.push(2);
        heap.push(3);
        assertFalse(heap.isEmpty());
        assertEquals(5, heap.size());

        assertEquals(1, heap.pop());
        assertEquals(4, heap.size());
        assertEquals(2, heap.pop());
        assertEquals(3, heap.pop());
        assertEquals(4, heap.pop());
        assertEquals(1, heap.size());
        assertEquals(5, heap.pop());
        assertEquals(0, heap.size());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testNegativeValuesAndDuplicates() {
        LongHeap heap = new LongHeap(8);
        heap.push(3);
        heap.push(-1);
        heap.push(3);
        heap.push(0);
        heap.push(-1);
        heap.push(Long.MIN_VALUE);
        heap.push(Long.MAX_VALUE);
        assertEquals(7, heap.size());

        assertEquals(Long.MIN_VALUE, heap.pop());
        assertEquals(-1, heap.pop());
        assertEquals(-1, heap.pop());
        assertEquals(0, heap.pop());
        assertEquals(3, heap.pop());
        assertEquals(3, heap.pop());
        assertEquals(Long.MAX_VALUE, heap.pop());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testSingleValue() {
        LongHeap heap = new LongHeap(4);
        heap.push(7);
        assertEquals(1, heap.size());
        assertEquals(7, heap.pop());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testGrowsBeyondTheCapacity() {
        LongHeap heap = new LongHeap(1);
        for (long value = 99; value >= 0; value--) {
            heap.push(value);
        }
        assertEquals(100, heap.size());
        for (long value = 0; value < 100; value++) {
            assertEquals(value, heap.pop());
        }
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testZeroCapacity() {
        LongHeap heap = new LongHeap(0);
        heap.push(2);
        heap.push(1);
        heap.push(3);
        assertEquals(1, heap.pop());
        assertEquals(2, heap.pop());
        assertEquals(3, heap.pop());
    }

    @Test
    public void testPopOnAnEmptyHeapThrows() {
        LongHeap heap = new LongHeap(4);
        assertThrows(IllegalStateException.class, heap::pop);

        heap.push(1);
        assertEquals(1, heap.pop());
        assertThrows(IllegalStateException.class, heap::pop);
    }

    @Test
    public void testClear() {
        LongHeap heap = new LongHeap(4);
        heap.push(3);
        heap.push(1);
        heap.push(2);
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        assertThrows(IllegalStateException.class, heap::pop);

        heap.push(5);
        heap.push(4);
        assertEquals(2, heap.size());
        assertEquals(4, heap.pop());
        assertEquals(5, heap.pop());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testInterleavedPushAndPop() {
        LongHeap heap = new LongHeap(2);
        heap.push(5);
        heap.push(2);
        assertEquals(2, heap.pop());
        heap.push(1);
        assertEquals(1, heap.pop());
        heap.push(7);
        heap.push(6);
        assertEquals(3, heap.size());
        assertEquals(5, heap.pop());
        assertEquals(6, heap.pop());
        assertEquals(7, heap.pop());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testPopsRandomValuesInSortedOrder() {
        Random random = new Random(42);
        long[] values = new long[1000];
        LongHeap heap = new LongHeap(4);
        for (int i = 0; i < values.length; i++) {
            // A small range, so that there are many duplicates and negative values.
            values[i] = random.nextInt(200) - 100;
            heap.push(values[i]);
        }
        assertEquals(values.length, heap.size());

        Arrays.sort(values);
        for (long value : values) {
            assertEquals(value, heap.pop());
        }
        assertTrue(heap.isEmpty());
    }
}
