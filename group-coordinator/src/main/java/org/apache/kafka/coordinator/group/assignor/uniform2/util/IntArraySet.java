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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An immutable {@link Set} of integers backed by a sorted array without duplicates. It is much
 * cheaper to build and to store than a {@code HashSet<Integer>}, which matters when many small
 * sets are built: building one copies a sorted array, and a lookup is a binary search.
 */
public final class IntArraySet extends AbstractSet<Integer> {
    private final int[] values;

    /**
     * @param values The values. The array must be sorted in ascending order, must not contain
     *               duplicates and must not be modified afterwards.
     */
    public IntArraySet(int[] values) {
        this.values = values;
    }

    /**
     * Creates a set from the first {@code length} values of the given array, which are copied.
     * The values must be sorted in ascending order and must not contain duplicates.
     */
    public static IntArraySet copyOf(int[] values, int length) {
        return new IntArraySet(Arrays.copyOf(values, length));
    }

    /**
     * @return The number of values.
     */
    @Override
    public int size() {
        return values.length;
    }

    /**
     * @return Whether the object is an {@link Integer} found in the values by binary search.
     */
    @Override
    public boolean contains(Object o) {
        if (o instanceof Integer value) {
            return Arrays.binarySearch(values, value) >= 0;
        }
        return false;
    }

    /**
     * @return An iterator over the values in ascending order.
     */
    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public Integer next() {
                if (index >= values.length) throw new NoSuchElementException();
                return values[index++];
            }
        };
    }

    /**
     * @return Whether the object is a {@link Set} holding the same integers, as the {@link Set}
     *         contract requires: another IntArraySet is compared array to array, any other set
     *         by size and lookups.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IntArraySet other) {
            return Arrays.equals(values, other.values);
        }
        if (!(o instanceof Set<?> otherSet)) return false;
        if (otherSet.size() != values.length) return false;
        for (int value : values) {
            if (!otherSet.contains(value)) return false;
        }
        return true;
    }

    /**
     * @return The sum of the values, which is the hash code the {@link Set} contract requires
     *         since the hash code of an {@link Integer} is the integer itself.
     */
    @Override
    public int hashCode() {
        var sum = 0;
        for (int value : values) {
            sum += value;
        }
        return sum;
    }

    /**
     * @return The values in ascending order, between brackets.
     */
    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
