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
 * The loads of the members while the extra partitions are distributed, with the members of every
 * cohort kept sorted by ascending load.
 *
 * <p>A load only changes by one at a time, which keeps the orders sorted in constant time. The
 * members of a cohort with the same load form a contiguous bucket of the order. A member whose
 * load grows swaps places with the last member of its bucket and the boundary with the next
 * bucket moves down over it; a member whose load shrinks swaps with the first member of its
 * bucket and the boundary with the previous bucket moves up over it.
 */
final class Loads {
    private final GroupModel model;
    /** Per member, its load: the number of partitions it gets with the current extra partitions. */
    final int[] load;
    /** Per cohort, its members by ascending load. */
    private final int[][] cohortOrder;
    /** Per member, its position in the order of its cohort. */
    private final int[] position;
    /** Per cohort, the first position of the members with a given load above the base load. */
    private final int[][] bucketStart;

    /**
     * @param model The group.
     * @param load  Per member, its load. The array is kept and updated in place.
     */
    Loads(GroupModel model, int[] load) {
        this.model = model;
        this.load = load;
        cohortOrder = new int[model.cohortCount][];
        bucketStart = new int[model.cohortCount][];
        position = new int[model.memberCount];
        for (int c = 0; c < model.cohortCount; c++) {
            cohortOrder[c] = new int[model.cohortSize[c]];
            // A member gets at most one extra partition per topic, so its load is at most the
            // base load plus its number of topics.
            bucketStart[c] = new int[model.cohortTopics[c].length + 2];
        }
        for (int m = 0; m < model.memberCount; m++) {
            int c = model.memberCohort[m];
            bucketStart[c][load[m] - model.cohortBaseLoad[c] + 1]++;
        }
        for (int c = 0; c < model.cohortCount; c++) {
            int[] start = bucketStart[c];
            for (int i = 1; i < start.length; i++) {
                start[i] += start[i - 1];
            }
        }
        int[][] fill = new int[model.cohortCount][];
        for (int c = 0; c < model.cohortCount; c++) {
            fill[c] = Arrays.copyOf(bucketStart[c], bucketStart[c].length - 1);
        }
        for (int m = 0; m < model.memberCount; m++) {
            int c = model.memberCohort[m];
            int at = fill[c][load[m] - model.cohortBaseLoad[c]]++;
            cohortOrder[c][at] = m;
            position[m] = at;
        }
    }

    /**
     * @return The members of the cohort by ascending load. The array is live: it changes with
     *         the loads and must not be modified by the caller.
     */
    int[] order(int cohort) {
        return cohortOrder[cohort];
    }

    void increment(int member) {
        int c = model.memberCohort[member];
        int[] start = bucketStart[c];
        int offset = load[member] - model.cohortBaseLoad[c];
        // Move the member to the end of its bucket, then shrink the next bucket over it.
        swap(cohortOrder[c], position[member], start[offset + 1] - 1);
        start[offset + 1]--;
        load[member]++;
    }

    void decrement(int member) {
        int c = model.memberCohort[member];
        int[] start = bucketStart[c];
        int offset = load[member] - model.cohortBaseLoad[c];
        // Move the member to the start of its bucket, then grow the previous bucket over it.
        swap(cohortOrder[c], position[member], start[offset]);
        start[offset]++;
        load[member]--;
    }

    /**
     * @return The smallest load in the group.
     */
    int min() {
        int min = Integer.MAX_VALUE;
        for (int c = 0; c < model.cohortCount; c++) {
            int[] order = cohortOrder[c];
            if (order.length > 0) {
                min = Math.min(min, load[order[0]]);
            }
        }
        return min;
    }

    private void swap(int[] order, int i, int j) {
        if (i == j) {
            return;
        }
        int mi = order[i];
        int mj = order[j];
        order[i] = mj;
        order[j] = mi;
        position[mj] = i;
        position[mi] = j;
    }
}
