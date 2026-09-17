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

import java.util.Arrays;

/**
 * A minimal binary min-heap of longs, avoiding the boxing of {@code PriorityQueue<Long>}.
 */
final class LongHeap {
    private long[] values;
    private int size;

    LongHeap(int capacity) {
        values = new long[Math.max(1, capacity)];
    }

    boolean isEmpty() {
        return size == 0;
    }

    int size() {
        return size;
    }

    void clear() {
        size = 0;
    }

    void push(long value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, size * 2);
        }
        int i = size++;
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (values[parent] <= value) {
                break;
            }
            values[i] = values[parent];
            i = parent;
        }
        values[i] = value;
    }

    /**
     * @return The smallest value, which is removed from the heap.
     * @throws IllegalStateException If the heap is empty.
     */
    long pop() {
        if (size == 0) {
            throw new IllegalStateException("The heap is empty");
        }
        long top = values[0];
        long last = values[--size];
        int i = 0;
        while (true) {
            int child = 2 * i + 1;
            if (child >= size) {
                break;
            }
            if (child + 1 < size && values[child + 1] < values[child]) {
                child++;
            }
            if (values[child] >= last) {
                break;
            }
            values[i] = values[child];
            i = child;
        }
        values[i] = last;
        return top;
    }
}
