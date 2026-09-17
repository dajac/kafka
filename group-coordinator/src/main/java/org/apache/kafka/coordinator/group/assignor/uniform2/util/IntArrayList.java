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

import java.util.Arrays;

/**
 * A minimal growable list of ints, which avoids the boxing of a {@code List<Integer>}: values
 * are stored in an array and appended without allocation until the array is full.
 */
public final class IntArrayList {
    private int[] values;
    private int size;

    /**
     * @param capacity The initial capacity, which may be zero. The list grows as needed.
     * @throws IllegalArgumentException If the capacity is negative.
     */
    public IntArrayList(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity: " + capacity);
        }
        values = new int[capacity];
    }

    /**
     * Appends the value, growing the list when it is full.
     */
    public void add(int value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, Math.max(8, size * 2));
        }
        values[size++] = value;
    }

    /**
     * @return The value at the index.
     * @throws IndexOutOfBoundsException If the index is negative or not below the size.
     */
    public int get(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " out of bounds for size " + size
            );
        }
        return values[index];
    }

    /**
     * @return The number of values.
     */
    public int size() {
        return size;
    }

    /**
     * @return Whether the list has no value.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Removes all the values, keeping the capacity.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Removes the first occurrence of the value, if any, shifting the following values down.
     */
    public void remove(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                System.arraycopy(values, i + 1, values, i, size - i - 1);
                size--;
                return;
            }
        }
    }

    /**
     * @return A copy of the values in insertion order, sized to the list.
     */
    public int[] toArray() {
        return Arrays.copyOf(values, size);
    }

    /**
     * @return A copy of the values sorted in ascending order. The list itself is not sorted.
     */
    public int[] toSortedArray() {
        var array = toArray();
        Arrays.sort(array);
        return array;
    }
}
