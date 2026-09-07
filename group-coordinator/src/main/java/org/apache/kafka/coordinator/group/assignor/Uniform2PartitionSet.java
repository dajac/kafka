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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;

/** Immutable sorted partition IDs, boxing only when the Set API requires it. */
final class Uniform2PartitionSet extends AbstractSet<Integer> {
    private final int[] values;

    private Uniform2PartitionSet(int[] values) {
        this.values = values;
    }

    /** The supplied slice must be sorted and contain no duplicates. */
    static Set<Integer> of(int[] values, int start, int end) {
        if (start == end) return Collections.emptySet();
        if (values[end - 1] - values[start] == end - start - 1) return new RangeSet(values[start], values[end - 1] + 1);
        // Own only this member's slice: sharing a whole topic array could retain many
        // obsolete partitions when other members reuse their previous assignments.
        return new Uniform2PartitionSet(Arrays.copyOfRange(values, start, end));
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public boolean contains(Object value) {
        return value instanceof Integer partition && Arrays.binarySearch(values, partition) >= 0;
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        return new PrimitiveIterator.OfInt() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public int nextInt() {
                if (!hasNext()) throw new NoSuchElementException();
                return values[index++];
            }
        };
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (int value : values) result += value;
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other instanceof Uniform2PartitionSet that) return Arrays.equals(values, that.values);
        return super.equals(other);
    }
}
