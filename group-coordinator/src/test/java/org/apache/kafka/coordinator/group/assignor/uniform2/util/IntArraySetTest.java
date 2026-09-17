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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntArraySetTest {

    @Test
    public void testEmptySet() {
        IntArraySet set = new IntArraySet(new int[0]);
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
        assertFalse(set.contains(0));
        assertFalse(set.iterator().hasNext());
        assertEquals(Set.of(), set);
        assertEquals(set, Set.of());
        assertEquals(Set.of().hashCode(), set.hashCode());
        assertEquals("[]", set.toString());
    }

    @Test
    public void testSizeAndIsEmpty() {
        IntArraySet single = new IntArraySet(new int[] {7});
        assertEquals(1, single.size());
        assertFalse(single.isEmpty());

        IntArraySet set = new IntArraySet(new int[] {1, 3, 5});
        assertEquals(3, set.size());
        assertFalse(set.isEmpty());
    }

    @Test
    public void testContains() {
        IntArraySet set = new IntArraySet(new int[] {-4, 1, 3, 5, 100});
        for (int value : new int[] {-4, 1, 3, 5, 100}) {
            assertTrue(set.contains(value), "should contain " + value);
        }
        for (int value : new int[] {-5, -3, 0, 2, 4, 6, 99, 101}) {
            assertFalse(set.contains(value), "should not contain " + value);
        }
        assertTrue(set.containsAll(List.of(1, 5)));
        assertTrue(set.containsAll(List.of()));
        assertFalse(set.containsAll(List.of(1, 2)));
    }

    @Test
    public void testContainsOfNonIntegerObjectsIsFalse() {
        IntArraySet set = new IntArraySet(new int[] {1, 3, 5});
        assertFalse(set.contains("3"));
        assertFalse(set.contains(3L));
        assertFalse(set.contains(3.0));
        assertFalse(set.contains((short) 3));
        assertFalse(set.contains(null));
    }

    @Test
    public void testContainsInALargeSet() {
        int[] values = new int[500];
        for (int i = 0; i < values.length; i++) {
            values[i] = 2 * i;
        }
        IntArraySet set = new IntArraySet(values);
        assertEquals(500, set.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i % 2 == 0, set.contains(i), "value " + i);
        }
        assertFalse(set.contains(-2));
        assertFalse(set.contains(1000));
    }

    @Test
    public void testIterationOrderIsAscending() {
        IntArraySet set = new IntArraySet(new int[] {-2, 0, 7, 9});
        assertEquals(List.of(-2, 0, 7, 9), new ArrayList<>(set));
        assertEquals(List.of(-2, 0, 7, 9), List.copyOf(set));
        assertArrayEquals(new Object[] {-2, 0, 7, 9}, set.toArray());

        Iterator<Integer> iterator = set.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(-2, iterator.next());
        assertEquals(0, iterator.next());
        assertEquals(7, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(9, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testEqualsAgainstOtherSets() {
        IntArraySet set = new IntArraySet(new int[] {1, 3, 5});
        assertEquals(set, set);
        assertEquals(Set.of(1, 3, 5), set);
        assertEquals(set, Set.of(1, 3, 5));
        assertEquals(set, new HashSet<>(List.of(5, 3, 1)));
        assertEquals(set, new TreeSet<>(List.of(1, 3, 5)));

        assertNotEquals(set, Set.of(1, 3));
        assertNotEquals(set, Set.of(1, 3, 5, 7));
        assertNotEquals(set, Set.of(1, 3, 6));
        assertNotEquals(Set.of(1, 3, 6), set);
        assertNotEquals(set, Set.of());
        // Only sets are equal to a set.
        assertNotEquals(set, List.of(1, 3, 5));
        assertNotEquals(set, "[1, 3, 5]");
        assertNotEquals(set, null);
    }

    @Test
    public void testEqualsAgainstAnotherIntArraySet() {
        IntArraySet set = new IntArraySet(new int[] {1, 3, 5});
        assertEquals(set, new IntArraySet(new int[] {1, 3, 5}));
        assertEquals(new IntArraySet(new int[] {1, 3, 5}), set);
        assertNotEquals(set, new IntArraySet(new int[] {1, 3}));
        assertNotEquals(set, new IntArraySet(new int[] {1, 3, 5, 7}));
        assertNotEquals(set, new IntArraySet(new int[] {1, 3, 6}));
        assertNotEquals(set, new IntArraySet(new int[0]));
        assertEquals(new IntArraySet(new int[0]), new IntArraySet(new int[0]));
    }

    @Test
    public void testHashCodeFollowsTheSetContract() {
        // The hash code of a set is the sum of the hash codes of its elements.
        assertEquals(9, new IntArraySet(new int[] {1, 3, 5}).hashCode());
        assertEquals(Set.of(1, 3, 5).hashCode(), new IntArraySet(new int[] {1, 3, 5}).hashCode());
        assertEquals(Set.of(-2, 0, 5).hashCode(), new IntArraySet(new int[] {-2, 0, 5}).hashCode());
        assertEquals(new IntArraySet(new int[] {1, 3, 5}).hashCode(), new IntArraySet(new int[] {1, 3, 5}).hashCode());

        // So it interoperates with hash based collections holding other set implementations.
        Set<Set<Integer>> sets = new HashSet<>();
        sets.add(Set.of(1, 3, 5));
        assertTrue(sets.contains(new IntArraySet(new int[] {1, 3, 5})));
        assertFalse(sets.contains(new IntArraySet(new int[] {1, 3})));

        Map<Set<Integer>, String> map = new HashMap<>();
        map.put(new IntArraySet(new int[] {2, 4}), "x");
        assertEquals("x", map.get(Set.of(2, 4)));
        assertEquals("x", map.get(new TreeSet<>(List.of(2, 4))));
    }

    @Test
    public void testCopyOf() {
        int[] values = {1, 2, 3, 99, 99};
        IntArraySet set = IntArraySet.copyOf(values, 3);
        assertEquals(Set.of(1, 2, 3), set);
        assertEquals(3, set.size());

        // The values are copied, so the set does not see later changes to the array.
        values[0] = 50;
        assertTrue(set.contains(1));
        assertFalse(set.contains(50));

        assertEquals(Set.of(), IntArraySet.copyOf(values, 0));
        assertTrue(IntArraySet.copyOf(values, 0).isEmpty());
    }

    @Test
    public void testMutatorsThrow() {
        IntArraySet set = new IntArraySet(new int[] {1, 3, 5});
        assertThrows(UnsupportedOperationException.class, () -> set.add(7));
        assertThrows(UnsupportedOperationException.class, () -> set.remove(3));
        assertThrows(UnsupportedOperationException.class, () -> set.addAll(List.of(7, 8)));
        assertThrows(UnsupportedOperationException.class, () -> set.removeAll(List.of(3)));
        assertThrows(UnsupportedOperationException.class, () -> set.retainAll(List.of(1)));
        assertThrows(UnsupportedOperationException.class, () -> set.removeIf(value -> value == 3));
        assertThrows(UnsupportedOperationException.class, set::clear);

        Iterator<Integer> iterator = set.iterator();
        iterator.next();
        assertThrows(UnsupportedOperationException.class, iterator::remove);

        // Nothing changed.
        assertEquals(Set.of(1, 3, 5), set);
    }

    @Test
    public void testToString() {
        assertEquals("[1, 3, 5]", new IntArraySet(new int[] {1, 3, 5}).toString());
        assertEquals("[-1]", new IntArraySet(new int[] {-1}).toString());
    }
}
