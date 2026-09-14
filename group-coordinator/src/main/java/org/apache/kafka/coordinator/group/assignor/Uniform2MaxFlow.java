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

import java.util.Arrays;

/**
 * A maximum flow from groups of partitions to racks, used by the rack aware partition phase to
 * align as many released partitions as possible with the members below their quota.
 *
 * <p>The network has a source, one node per group of partitions sharing the same replica racks,
 * one node per rack and a sink. A group can flow to each of its racks, up to the number of its
 * partitions, and a rack can flow to the sink up to the demand of its members. The maximum flow
 * is the largest number of partitions that can be handed to a member in one of their replica
 * racks. The network is small in practice, a handful of nodes with the usual three racks, so a
 * plain Edmonds-Karp search on a dense capacity matrix is used; its cost grows with the square
 * of the number of nodes, which the caller bounds.
 */
final class Uniform2MaxFlow {
    private static final int NONE = -1;

    private Uniform2MaxFlow() { }

    /**
     * @param groupRacks Per group, the racks of its partitions, one bit per rack.
     * @param supply     Per group, its number of partitions.
     * @param demand     Per rack, the number of partitions its members are short of.
     * @return Per group and rack, the number of partitions of the group to hand to the rack.
     */
    static int[][] compute(long[] groupRacks, int[] supply, int[] demand) {
        int groupCount = groupRacks.length;
        int rackCount = demand.length;
        int source = groupCount + rackCount;
        int sink = source + 1;
        int nodes = sink + 1;
        int[][] capacity = new int[nodes][nodes];
        for (int group = 0; group < groupCount; group++) {
            capacity[source][group] = supply[group];
            long racks = groupRacks[group];
            while (racks != 0) {
                capacity[group][groupCount + Long.numberOfTrailingZeros(racks)] = supply[group];
                racks &= racks - 1;
            }
        }
        for (int rack = 0; rack < rackCount; rack++) {
            capacity[groupCount + rack][sink] = demand[rack];
        }

        int[] parent = new int[nodes];
        int[] queue = new int[nodes];
        while (findAugmentingPath(capacity, source, sink, parent, queue)) {
            int bottleneck = Integer.MAX_VALUE;
            for (int v = sink; v != source; v = parent[v]) {
                bottleneck = Math.min(bottleneck, capacity[parent[v]][v]);
            }
            for (int v = sink; v != source; v = parent[v]) {
                capacity[parent[v]][v] -= bottleneck;
                capacity[v][parent[v]] += bottleneck;
            }
        }

        int[][] flow = new int[groupCount][rackCount];
        for (int group = 0; group < groupCount; group++) {
            long racks = groupRacks[group];
            while (racks != 0) {
                int rack = Long.numberOfTrailingZeros(racks);
                racks &= racks - 1;
                // The residual capacity of the reverse edge is the flow on the edge.
                flow[group][rack] = capacity[groupCount + rack][group];
            }
        }
        return flow;
    }

    /**
     * Breadth-first search of a path with remaining capacity from the source to the sink.
     */
    private static boolean findAugmentingPath(int[][] capacity, int source, int sink, int[] parent, int[] queue) {
        Arrays.fill(parent, NONE);
        parent[source] = source;
        int head = 0;
        int tail = 0;
        queue[tail++] = source;
        while (head < tail && parent[sink] == NONE) {
            int u = queue[head++];
            for (int v = 0; v < capacity.length; v++) {
                if (parent[v] == NONE && capacity[u][v] > 0) {
                    parent[v] = u;
                    queue[tail++] = v;
                }
            }
        }
        return parent[sink] != NONE;
    }
}
