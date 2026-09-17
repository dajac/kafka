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
 * A minimal growable list of ints, avoiding the boxing of {@code List<Integer>}.
 */
public final class IntArrayList {
    private int[] values;
    private int size;

    public IntArrayList(int capacity) {
        values = new int[Math.max(1, capacity)];
    }

    public void add(int value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, Math.max(8, size * 2));
        }
        values[size++] = value;
    }

    public int get(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }
        return values[index];
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void clear() {
        size = 0;
    }

    /**
     * Removes the first occurrence of the value, if any, shifting the following values down.
     */
    public void removeValue(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                System.arraycopy(values, i + 1, values, i, size - i - 1);
                size--;
                return;
            }
        }
    }

    public int[] toArray() {
        return Arrays.copyOf(values, size);
    }

    public int[] toSortedArray() {
        int[] array = toArray();
        Arrays.sort(array);
        return array;
    }
}
