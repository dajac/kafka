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

import org.apache.kafka.common.Uuid;

import java.util.Arrays;

/**
 * Maps topic ids to dense indices, with open addressing on the two longs of the id. It is much
 * cheaper than a {@code HashMap<Uuid, Integer>} for the many lookups done per member.
 */
final class TopicIndex {
    /**
     * Returned by {@link #indexOf} for an unknown topic.
     */
    static final int NONE = -1;

    private final long[] mostSignificantBits;
    private final long[] leastSignificantBits;
    private final int[] indices;
    private final int mask;

    /**
     * @param topicIds The topics, which get the indices {@code 0} to {@code topicIds.length - 1}.
     */
    TopicIndex(Uuid[] topicIds) {
        int capacity = Integer.highestOneBit(Math.max(2, topicIds.length * 2 - 1)) << 1;
        mostSignificantBits = new long[capacity];
        leastSignificantBits = new long[capacity];
        indices = new int[capacity];
        Arrays.fill(indices, NONE);
        mask = capacity - 1;
        for (int t = 0; t < topicIds.length; t++) {
            long msb = topicIds[t].getMostSignificantBits();
            long lsb = topicIds[t].getLeastSignificantBits();
            int slot = slot(msb, lsb);
            while (indices[slot] != NONE) {
                slot = (slot + 1) & mask;
            }
            mostSignificantBits[slot] = msb;
            leastSignificantBits[slot] = lsb;
            indices[slot] = t;
        }
    }

    /**
     * @return The index of the topic, or {@link #NONE} if it is unknown.
     */
    int indexOf(Uuid topicId) {
        long msb = topicId.getMostSignificantBits();
        long lsb = topicId.getLeastSignificantBits();
        int slot = slot(msb, lsb);
        while (true) {
            int index = indices[slot];
            if (index == NONE) {
                return NONE;
            }
            if (mostSignificantBits[slot] == msb && leastSignificantBits[slot] == lsb) {
                return index;
            }
            slot = (slot + 1) & mask;
        }
    }

    private int slot(long msb, long lsb) {
        long hash = (msb ^ lsb) * 0x9E3779B97F4A7C15L;
        return (int) (hash >>> 32) & mask;
    }
}
