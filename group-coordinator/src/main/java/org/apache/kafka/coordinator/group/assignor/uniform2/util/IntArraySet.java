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
 * An immutable {@link Set} of integers backed by a sorted array without duplicates.
 * It is much cheaper to build and to store than a {@code HashSet<Integer>}, which
 * matters when an assignment contains many small partition sets.
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

    @Override
    public int size() {
        return values.length;
    }


    @Override
    public boolean contains(Object o) {
        if (o instanceof Integer value) {
            return Arrays.binarySearch(values, value) >= 0;
        }
        return false;
    }

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

    @Override
    public int hashCode() {
        // The hash code of a Set is defined as the sum of the hash codes of its elements,
        // and the hash code of an Integer is the integer itself.
        int sum = 0;
        for (int value : values) {
            sum += value;
        }
        return sum;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

}
