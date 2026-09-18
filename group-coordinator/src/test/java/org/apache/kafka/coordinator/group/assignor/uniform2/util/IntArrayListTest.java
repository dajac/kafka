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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntArrayListTest {

    @Test
    public void testNewListIsEmpty() {
        var list = new IntArrayList(4);
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        assertArrayEquals(new int[0], list.toArray());
        assertArrayEquals(new int[0], list.toSortedArray());
    }

    @Test
    public void testAddAndGet() {
        var list = new IntArrayList(4);
        list.add(5);
        list.add(-3);
        list.add(9);
        assertFalse(list.isEmpty());
        assertEquals(3, list.size());
        assertEquals(5, list.get(0));
        assertEquals(-3, list.get(1));
        assertEquals(9, list.get(2));
        assertArrayEquals(new int[] {5, -3, 9}, list.toArray());
    }

    @Test
    public void testGrowsBeyondTheInitialCapacity() {
        var list = new IntArrayList(2);
        for (int i = 0; i < 100; i++) {
            list.add(i * 3);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i * 3, list.get(i));
        }
    }

    @Test
    public void testNegativeCapacityThrows() {
        assertThrows(IllegalArgumentException.class, () -> new IntArrayList(-1));
    }

    @Test
    public void testZeroCapacity() {
        var list = new IntArrayList(0);
        list.add(1);
        list.add(2);
        assertEquals(2, list.size());
        assertArrayEquals(new int[] {1, 2}, list.toArray());
    }

    @Test
    public void testGetPastTheSizeThrows() {
        var list = new IntArrayList(4);
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(0));
        list.add(1);
        list.add(2);
        assertEquals(2, list.get(1));
        // Within the capacity but past the size.
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(2));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(3));
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(100));
    }

    @Test
    public void testClear() {
        var list = new IntArrayList(4);
        list.add(1);
        list.add(2);
        list.add(3);
        list.clear();
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        assertArrayEquals(new int[0], list.toArray());
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(0));

        list.add(7);
        assertEquals(1, list.size());
        assertEquals(7, list.get(0));
        assertArrayEquals(new int[] {7}, list.toArray());
    }

    @Test
    public void testRemoveRemovesTheFirstOccurrenceOnly() {
        var list = new IntArrayList(4);
        list.add(4);
        list.add(7);
        list.add(4);
        list.add(9);

        list.remove(4);
        assertArrayEquals(new int[] {7, 4, 9}, list.toArray());
        assertEquals(3, list.size());

        // The last value.
        list.remove(9);
        assertArrayEquals(new int[] {7, 4}, list.toArray());

        // The first value.
        list.remove(7);
        assertArrayEquals(new int[] {4}, list.toArray());

        list.remove(4);
        assertTrue(list.isEmpty());
        assertArrayEquals(new int[0], list.toArray());
    }

    @Test
    public void testRemoveOfAnAbsentValueIsANoOp() {
        var list = new IntArrayList(4);
        list.remove(1);
        assertTrue(list.isEmpty());

        list.add(1);
        list.add(2);
        list.add(3);
        list.remove(5);
        assertEquals(3, list.size());
        assertArrayEquals(new int[] {1, 2, 3}, list.toArray());
    }

    @Test
    public void testToArrayIsACopySizedToTheList() {
        var list = new IntArrayList(16);
        list.add(1);
        list.add(2);
        var array = list.toArray();
        assertEquals(2, array.length);
        array[0] = 99;
        assertEquals(1, list.get(0));
    }

    @Test
    public void testToSortedArrayDoesNotChangeTheList() {
        var list = new IntArrayList(4);
        list.add(5);
        list.add(1);
        list.add(4);
        list.add(1);
        list.add(-3);
        assertArrayEquals(new int[] {-3, 1, 1, 4, 5}, list.toSortedArray());
        assertArrayEquals(new int[] {5, 1, 4, 1, -3}, list.toArray());
        assertEquals(5, list.size());
    }
}
