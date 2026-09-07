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
 * Warm-started convex circulation with exact lexicographic residual-cycle repair.
 * Costs are four integer coordinates; no weighted approximation or other solver is used.
 */
final class Uniform3AssignmentGraph {
    final int[] flow;
    private final int[] from;
    private final int[] to;
    private final int[] upper;
    private final int[] free;
    private final int[] convex;
    private final int[] remote;
    private final long[][] distance;
    private final int[] predecessor;
    private final int[] depth;
    private final int[] queue;
    private final boolean[] queued;
    private int[] offsets;
    private int[] arcs;
    private int nodes;
    private int edges;

    Uniform3AssignmentGraph(int nodeCapacity, int edgeCapacity) {
        from = new int[edgeCapacity];
        to = new int[edgeCapacity];
        upper = new int[edgeCapacity];
        flow = new int[edgeCapacity];
        free = new int[edgeCapacity];
        convex = new int[edgeCapacity];
        remote = new int[edgeCapacity];
        distance = new long[4][nodeCapacity];
        predecessor = new int[nodeCapacity];
        depth = new int[nodeCapacity];
        queue = new int[nodeCapacity + 1];
        queued = new boolean[nodeCapacity];
    }

    int node() {
        return nodes++;
    }

    int edge(int source, int target, int capacity, int initial, int balance, int rack, int retained) {
        int edge = edges++;
        from[edge] = source;
        to[edge] = target;
        upper[edge] = capacity;
        flow[edge] = initial;
        convex[edge] = balance;
        remote[edge] = rack;
        free[edge] = retained;
        return edge;
    }

    private int destination(int arc) {
        return (arc & 1) == 0 ? to[arc >>> 1] : from[arc >>> 1];
    }

    private int capacity(int arc) {
        return (arc & 1) == 0 ? upper[arc >>> 1] - flow[arc >>> 1] : flow[arc >>> 1];
    }

    private long cost(int arc, int coordinate, int offset) {
        int edge = arc >>> 1;
        boolean forward = (arc & 1) == 0;
        int position = forward ? flow[edge] + offset : flow[edge] - offset - 1;
        long cost;
        if (coordinate == convex[edge]) cost = 2L * position + 1;
        else if (coordinate == 2) cost = remote[edge];
        else if (coordinate == 3) cost = position < free[edge] ? 0 : 1;
        else cost = 0;
        return forward ? cost : -cost;
    }

    void optimize() {
        offsets = new int[nodes + 1];
        for (int edge = 0; edge < edges; edge++) {
            offsets[from[edge] + 1]++;
            offsets[to[edge] + 1]++;
        }
        for (int node = 1; node <= nodes; node++) offsets[node] += offsets[node - 1];
        int[] cursor = Arrays.copyOf(offsets, nodes);
        arcs = new int[Math.multiplyExact(edges, 2)];
        for (int edge = 0; edge < edges; edge++) {
            arcs[cursor[from[edge]]++] = 2 * edge;
            arcs[cursor[to[edge]]++] = 2 * edge + 1;
        }
        int[] cycle = new int[nodes];
        int length;
        while ((length = negativeCycle(cycle)) > 0) {
            int limit = Integer.MAX_VALUE;
            for (int i = 0; i < length; i++) limit = Math.min(limit, capacity(cycle[i]));
            int low = 1;
            int high = limit;
            while (low < high) {
                int middle = low + (high - low + 1) / 2;
                if (improves(cycle, length, middle - 1)) low = middle;
                else high = middle - 1;
            }
            for (int i = 0; i < length; i++) flow[cycle[i] >>> 1] += (cycle[i] & 1) == 0 ? low : -low;
        }
    }

    private boolean improves(int[] cycle, int length, int offset) {
        for (int coordinate = 0; coordinate < 4; coordinate++) {
            long cost = 0;
            for (int i = 0; i < length; i++) cost += cost(cycle[i], coordinate, offset);
            if (cost != 0) return cost < 0;
        }
        return false;
    }

    private boolean relax(int node, int arc) {
        int target = destination(arc);
        boolean better = false;
        for (int coordinate = 0; coordinate < 4; coordinate++) {
            long candidate = distance[coordinate][node] + cost(arc, coordinate, 0);
            if (candidate != distance[coordinate][target]) {
                better = candidate < distance[coordinate][target];
                break;
            }
        }
        if (!better) return false;
        for (int coordinate = 0; coordinate < 4; coordinate++) distance[coordinate][target] = distance[coordinate][node] + cost(arc, coordinate, 0);
        predecessor[target] = arc;
        depth[target] = depth[node] + 1;
        return true;
    }

    private int extract(int node, int[] cycle) {
        for (int i = 0; i < nodes; i++) {
            if (predecessor[node] < 0) return 0;
            node = destination(predecessor[node] ^ 1);
        }
        int start = node;
        int length = 0;
        do {
            cycle[length++] = predecessor[node];
            node = destination(predecessor[node] ^ 1);
        } while (node != start);
        return improves(cycle, length, 0) ? length : 0;
    }

    private int shortCycle(int node, int[] cycle) {
        int start = node;
        for (int length = 1; length <= Math.min(nodes, 32); length++) {
            int arc = predecessor[node];
            if (arc < 0) return 0;
            cycle[length - 1] = arc;
            node = destination(arc ^ 1);
            if (node == start) return improves(cycle, length, 0) ? length : 0;
        }
        return 0;
    }

    private int checkCycle(int target, int relaxations, int[] cycle) {
        // Large graphs often need a short exchange. Do not circulate labels
        // around that cycle |V| times before recognizing it. This bounded
        // probe adds constant amortized work; the general check remains.
        if ((relaxations & 15) == 0) {
            int length = shortCycle(target, cycle);
            if (length > 0) return length;
        }
        if (depth[target] >= nodes) {
            int length = extract(target, cycle);
            if (length > 0) return length;
            depth[target] = 0;
        }
        return 0;
    }

    private int negativeCycle(int[] cycle) {
        for (long[] values : distance) Arrays.fill(values, 0);
        Arrays.fill(depth, 0);
        Arrays.fill(predecessor, -1);
        Arrays.fill(queued, true);
        int head = 0;
        int tail = nodes;
        int relaxations = 0;
        for (int node = 0; node < nodes; node++) queue[node] = node;
        while (head != tail) {
            int node = queue[head];
            if (++head == queue.length) head = 0;
            queued[node] = false;
            for (int i = offsets[node]; i < offsets[node + 1]; i++) {
                int arc = arcs[i];
                if (capacity(arc) == 0 || !relax(node, arc)) continue;
                int target = destination(arc);
                int length = checkCycle(target, ++relaxations, cycle);
                if (length > 0) return length;
                if (!queued[target]) {
                    queue[tail] = target;
                    if (++tail == queue.length) tail = 0;
                    queued[target] = true;
                }
            }
        }
        return 0;
    }
}
