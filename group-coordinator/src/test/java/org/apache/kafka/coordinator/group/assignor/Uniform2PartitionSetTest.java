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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Uniform2PartitionSetTest {
    @Test
    void matchesHashSetContract() {
        Random random = new Random(771);
        for (int trial = 0; trial < 1000; trial++) {
            Set<Integer> expected = new HashSet<>();
            for (int count = random.nextInt(40); count > 0; count--) expected.add(random.nextInt(1000));
            int[] values = expected.stream().mapToInt(Integer::intValue).sorted().toArray();
            Set<Integer> actual = Uniform2PartitionSet.of(values, 0, values.length);
            assertEquals(expected, actual);
            assertEquals(actual, expected);
            assertEquals(expected.hashCode(), actual.hashCode());
            assertEquals(expected, new HashSet<>(actual));
            assertArrayEquals(Arrays.stream(values).boxed().toArray(Integer[]::new), actual.toArray(new Integer[0]));
            assertFalse(actual.contains(null));
            assertFalse(actual.contains("0"));
            for (int value = 0; value < 1000; value++) assertEquals(expected.contains(value), actual.contains(value));
            Arrays.fill(values, -1);
            assertEquals(expected, actual, "The set must not retain a mutable input array");
        }
    }

    @Test
    void usesRangesAndEnforcesIteratorContract() {
        assertInstanceOf(RangeSet.class, Uniform2PartitionSet.of(new int[] {7, 8, 9}, 0, 3));
        assertEquals(Set.of(4, 8, 12), Uniform2PartitionSet.of(new int[] {0, 4, 8, 12, 20}, 1, 4));
        Set<Integer> set = Uniform2PartitionSet.of(new int[] {4, 8, Integer.MAX_VALUE - 1}, 0, 3);
        Iterator<Integer> iterator = set.iterator();
        assertEquals(4, iterator.next());
        assertThrows(UnsupportedOperationException.class, iterator::remove);
        assertEquals(8, iterator.next());
        assertEquals(Integer.MAX_VALUE - 1, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertThrows(UnsupportedOperationException.class, () -> set.add(5));
        assertThrows(UnsupportedOperationException.class, () -> set.remove(4));
        assertTrue(set.contains(Integer.MAX_VALUE - 1));
        assertEquals(Set.of(4, 8, Integer.MAX_VALUE - 1).hashCode(), set.hashCode());
    }
}
