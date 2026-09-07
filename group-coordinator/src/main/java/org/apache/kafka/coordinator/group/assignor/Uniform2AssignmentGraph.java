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
 * Integral convex-cost flow with lexicographic objectives. Edge handles are integer
 * indices into parallel primitive arrays. Residual arc 2e is forward and 2e+1 is reverse.
 * CSR adjacency preserves edge insertion order, including deterministic tie breaking.
 * Marginal costs represent whole convex ranges rather than one edge per partition.
 */
final class Uniform2AssignmentGraph {
    enum Objective { TOTAL, TOPIC, RACK, MOVEMENT }

    final int[] lower;
    final int[] upper;
    final int[] flow;
    private final int[] from;
    private final int[] to;
    private final int[] free;
    private final byte[] balance;
    private final byte[] remote;
    private final int[] supplies;
    private int nodes;
    private int edges;
    private int[] offsets;
    private int[] arcs;

    Uniform2AssignmentGraph(int nodeCapacity, int edgeCapacity) {
        supplies = new int[nodeCapacity];
        lower = new int[edgeCapacity];
        upper = new int[edgeCapacity];
        flow = new int[edgeCapacity];
        from = new int[edgeCapacity];
        to = new int[edgeCapacity];
        free = new int[edgeCapacity];
        balance = new byte[edgeCapacity];
        remote = new byte[edgeCapacity];
    }

    int node(int supply) {
        supplies[nodes] = supply;
        return nodes++;
    }

    int edge(int source, int target, int capacity, Objective objective, int remoteCost, int freeCapacity) {
        int edge = edges++;
        from[edge] = source;
        to[edge] = target;
        upper[edge] = capacity;
        balance[edge] = objective == null ? -1 : (byte) objective.ordinal();
        remote[edge] = (byte) remoteCost;
        free[edge] = freeCapacity;
        return edge;
    }

    private void adjacency() {
        if (offsets != null) return;
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
    }

    private int destination(int arc) {
        return (arc & 1) == 0 ? to[arc >>> 1] : from[arc >>> 1];
    }

    private int residual(int arc) {
        int edge = arc >>> 1;
        return (arc & 1) == 0 ? upper[edge] - flow[edge] : flow[edge] - lower[edge];
    }

    private long edgeCost(int edge, Objective objective, int position) {
        if (objective.ordinal() == balance[edge]) return 2L * position + 1;
        if (objective == Objective.RACK) return remote[edge];
        if (objective == Objective.MOVEMENT) return position < free[edge] ? 0 : 1;
        return 0;
    }

    private long cost(int arc, Objective objective, int offset) {
        int edge = arc >>> 1;
        return (arc & 1) == 0 ? edgeCost(edge, objective, flow[edge] + offset) :
            -edgeCost(edge, objective, flow[edge] - offset - 1);
    }

    /** An indexed heap: at most one entry per node, with no boxed keys or visits. */
    static final class NodeHeap {
        private final long[] keys;
        private final int[] heap;
        private final int[] position;
        private int size;

        NodeHeap(long[] keys) {
            this.keys = keys;
            heap = new int[keys.length];
            position = new int[keys.length];
            Arrays.fill(position, -1);
        }

        void clear() {
            while (size > 0) position[heap[--size]] = -1;
        }

        boolean isEmpty() {
            return size == 0;
        }

        private boolean before(int a, int b) {
            return keys[a] < keys[b] || (keys[a] == keys[b] && a < b);
        }

        void addOrDecrease(int node) {
            int index = position[node];
            if (index < 0) index = size++;
            while (index > 0) {
                int parent = (index - 1) >>> 1;
                if (!before(node, heap[parent])) break;
                heap[index] = heap[parent];
                position[heap[index]] = index;
                index = parent;
            }
            heap[index] = node;
            position[node] = index;
        }

        int remove() {
            int result = heap[0];
            int last = heap[--size];
            position[result] = -1;
            if (size == 0) return result;
            int index = 0;
            while (index < size / 2) {
                int child = 2 * index + 1;
                if (child + 1 < size && before(heap[child + 1], heap[child])) child++;
                if (!before(heap[child], last)) break;
                heap[index] = heap[child];
                position[heap[index]] = index;
                index = child;
            }
            heap[index] = last;
            position[last] = index;
            return result;
        }
    }

    private static final class NodeQueue {
        private final int[] values;
        private int head;
        private int tail;

        NodeQueue(int capacity) {
            values = new int[capacity + 1];
        }

        boolean isEmpty() {
            return head == tail;
        }

        void add(int node) {
            values[tail] = node;
            if (++tail == values.length) tail = 0;
        }

        int remove() {
            int node = values[head];
            if (++head == values.length) head = 0;
            return node;
        }
    }

    /** Find a zero-cost feasible flow before invoking the cost optimizer. */
    boolean localFlow() {
        adjacency();
        int[] savedUpper = Arrays.copyOf(upper, edges);
        int[] savedFlow = Arrays.copyOf(flow, edges);
        long[] excess = new long[nodes];
        for (int node = 0; node < excess.length; node++) excess[node] = supplies[node];
        for (int edge = 0; edge < edges; edge++) {
            if (remote[edge] != 0) upper[edge] = 0;
            flow[edge] = Math.min(flow[edge], upper[edge]);
            excess[from[edge]] -= flow[edge];
            excess[to[edge]] += flow[edge];
        }
        int[] level = new int[excess.length];
        int[] current = new int[excess.length];
        int[] path = new int[excess.length];
        int[] queue = new int[excess.length];
        while (levels(excess, level, queue, null)) {
            System.arraycopy(offsets, 0, current, 0, nodes);
            for (int node = 0; node < excess.length; node++) {
                while (excess[node] > 0 && augment(node, excess, level, current, path, null)) {
                    // Exhaust the shortest residual paths from this supply node.
                }
            }
        }
        for (long value : excess) {
            if (value != 0) {
                System.arraycopy(savedUpper, 0, upper, 0, edges);
                System.arraycopy(savedFlow, 0, flow, 0, edges);
                return false;
            }
        }
        return true;
    }

    private boolean levels(long[] excess, int[] level, int[] queue, long[] price) {
        Arrays.fill(level, -1);
        int head = 0;
        int tail = 0;
        for (int node = 0; node < excess.length; node++) {
            if (excess[node] > 0) {
                level[node] = 0;
                queue[tail++] = node;
            }
        }
        boolean reachable = false;
        while (head < tail) {
            int node = queue[head++];
            if (excess[node] < 0) {
                reachable = true;
                continue;
            }
            for (int index = offsets[node]; index < offsets[node + 1]; index++) {
                int arc = arcs[index];
                if (capacity(arc, node, price) > 0 && level[destination(arc)] < 0) {
                    level[destination(arc)] = level[node] + 1;
                    queue[tail++] = destination(arc);
                }
            }
        }
        return reachable;
    }

    // Iterative blocking-flow traversal avoids a Java stack frame per member in long
    // transfer chains created by heterogeneous subscriptions.
    private boolean augment(int start, long[] excess, int[] level, int[] current, int[] path, long[] price) {
        int depth = 0;
        path[0] = start;
        while (depth >= 0) {
            int node = path[depth];
            if (excess[node] < 0) {
                pushPath(start, node, depth, excess, current, path, price);
                return true;
            }
            while (current[node] < offsets[node + 1]) {
                int arc = arcs[current[node]];
                if (capacity(arc, node, price) > 0 && level[destination(arc)] == level[node] + 1) break;
                current[node]++;
            }
            if (current[node] == offsets[node + 1]) {
                level[node] = -1;
                depth--;
            } else {
                path[++depth] = destination(arcs[current[node]]);
            }
        }
        return false;
    }

    private void pushPath(int start, int end, int depth, long[] excess, int[] current, int[] path, long[] price) {
        int amount = (int) Math.min(excess[start], -excess[end]);
        for (int index = 0; index < depth; index++) {
            amount = Math.min(amount, capacity(arcs[current[path[index]]], path[index], price));
        }
        for (int index = 0; index < depth; index++) {
            int from = path[index];
            push(from, arcs[current[from]], amount, excess);
        }
    }

    private int capacity(int arc, int from, long[] price) {
        return price == null ? residual(arc) :
            admissible(arc, Objective.MOVEMENT, 1, price[from] - price[destination(arc)], true);
    }

    /**
     * Primal-dual shortest paths with blocking flows for the final 0/1 movement cost.
     * Starting at lower bounds has nonnegative residual costs. Each shortest-path
     * update preserves dual feasibility, and each blocking flow fills all zero reduced
     * cost paths in bulk. In particular, retained partitions flow before moved ones.
     */
    void minimizeMovement() {
        adjacency();
        int size = nodes;
        long[] excess = new long[size];
        long[] price = new long[size];
        for (int node = 0; node < size; node++) excess[node] = supplies[node];
        for (int edge = 0; edge < edges; edge++) {
            flow[edge] = lower[edge];
            excess[from[edge]] -= flow[edge];
            excess[to[edge]] += flow[edge];
        }
        int[] level = new int[size];
        int[] current = new int[size];
        int[] path = new int[size];
        int[] queue = new int[size];
        long[] distance = new long[size];
        NodeHeap heap = new NodeHeap(distance);
        while (movementPrices(excess, price, distance, heap)) {
            while (levels(excess, level, queue, price)) {
                System.arraycopy(offsets, 0, current, 0, nodes);
                for (int node = 0; node < size; node++) {
                    while (excess[node] > 0 && augment(node, excess, level, current, path, price)) {
                        // Fill all shortest paths, including residual transfer chains.
                    }
                }
            }
        }
    }

    private boolean movementPrices(long[] excess, long[] price, long[] distance, NodeHeap queue) {
        queue.clear();
        Arrays.fill(distance, Long.MAX_VALUE);
        for (int node = 0; node < excess.length; node++) {
            if (excess[node] > 0) {
                distance[node] = 0;
                queue.addOrDecrease(node);
            }
        }
        if (queue.isEmpty()) return false;
        long limit = Long.MAX_VALUE;
        while (!queue.isEmpty()) {
            int node = queue.remove();
            if (excess[node] < 0) {
                limit = distance[node];
                break;
            }
            for (int index = offsets[node]; index < offsets[node + 1]; index++) {
                int arc = arcs[index];
                if (residual(arc) == 0) continue;
                long candidate = distance[node] + cost(arc, Objective.MOVEMENT, 0) + price[node] - price[destination(arc)];
                if (candidate < distance[destination(arc)]) {
                    distance[destination(arc)] = candidate;
                    queue.addOrDecrease(destination(arc));
                }
            }
        }
        updateMovementPrices(price, distance, limit);
        return true;
    }

    private void updateMovementPrices(long[] price, long[] distance, long limit) {
        if (limit == Long.MAX_VALUE) throw new IllegalStateException("Infeasible movement graph");
        for (int node = 0; node < price.length; node++) price[node] += Math.min(distance[node], limit);
    }

    void optimize(Objective objective) {
        adjacency();
        int size = nodes;
        long scale = size + 1L;
        long[] price = new long[size];
        long[] excess = new long[size];
        for (int node = 0; node < size; node++) excess[node] = supplies[node];
        long maximum = 1;
        for (int edge = 0; edge < edges; edge++) {
            excess[from[edge]] -= flow[edge];
            excess[to[edge]] += flow[edge];
            maximum = Math.max(maximum, Math.abs(edgeCost(edge, objective, upper[edge])) * scale);
        }
        long epsilon = maximum;
        do {
            epsilon = Math.max(1, epsilon / 16);
            refine(objective, scale, epsilon, price, excess);
        } while (epsilon > 1);
        if (objective != Objective.MOVEMENT) restrict(objective, scale, price);
    }

    private void refine(Objective objective, long scale, long epsilon, long[] price, long[] excess) {
        for (int node = 0; node < nodes; node++) {
            for (int index = offsets[node]; index < offsets[node + 1]; index++) {
                int arc = arcs[index];
                int amount = admissible(arc, objective, scale, price[node] - price[destination(arc)], false);
                push(node, arc, amount, excess);
            }
        }
        NodeQueue active = new NodeQueue(nodes);
        boolean[] queued = new boolean[nodes];
        int[] current = Arrays.copyOf(offsets, nodes);
        for (int node = 0; node < nodes; node++) {
            if (excess[node] > 0) {
                active.add(node);
                queued[node] = true;
            }
        }
        while (!active.isEmpty()) {
            int node = active.remove();
            queued[node] = false;
            while (excess[node] > 0) {
                if (current[node] == offsets[node + 1]) {
                    long minimum = Long.MAX_VALUE;
                    for (int index = offsets[node]; index < offsets[node + 1]; index++) {
                        int arc = arcs[index];
                        if (residual(arc) > 0) {
                            minimum = Math.min(minimum, cost(arc, objective, 0) * scale + price[node] - price[destination(arc)]);
                        }
                    }
                    if (minimum == Long.MAX_VALUE) throw new IllegalStateException("Infeasible assignment graph");
                    price[node] -= minimum + epsilon;
                    current[node] = offsets[node];
                    continue;
                }
                int arc = arcs[current[node]];
                int amount = (int) Math.min(excess[node],
                    admissible(arc, objective, scale, price[node] - price[destination(arc)], false));
                if (amount == 0) {
                    current[node]++;
                    continue;
                }
                push(node, arc, amount, excess);
                if (excess[destination(arc)] > 0 && !queued[destination(arc)]) {
                    queued[destination(arc)] = true;
                    active.add(destination(arc));
                }
            }
        }
    }

    private void push(int node, int arc, int amount, long[] excess) {
        flow[arc >>> 1] += (arc & 1) == 0 ? amount : -amount;
        excess[node] -= amount;
        excess[destination(arc)] += amount;
    }

    /** Number of residual units with negative (or nonpositive) reduced marginal cost. */
    private int admissible(int arc, Objective objective, long scale, long difference, boolean includeZero) {
        int capacity = residual(arc);
        long threshold = includeZero ? 0 : -1;
        if (capacity == 0 || cost(arc, objective, 0) * scale + difference > threshold) return 0;
        if (cost(arc, objective, capacity - 1) * scale + difference <= threshold) return capacity;
        int low = 1;
        int high = capacity;
        while (low < high) {
            int middle = low + (high - low) / 2;
            if (cost(arc, objective, middle) * scale + difference <= threshold) low = middle + 1;
            else high = middle;
        }
        return low;
    }

    /**
     * Integer costs scaled by |V|+1 and epsilon=1 rule out negative residual cycles.
     * Recover exact dual prices with shortest paths, then keep only zero-cost residual
     * capacity. These are precisely the flows preserving the objective's optimum.
     */
    private void restrict(Objective objective, long scale, long[] price) {
        int size = nodes;
        for (int node = 0; node < size; node++) price[node] = Math.floorDiv(price[node], scale);
        long[] distance = new long[size];
        boolean[] queued = new boolean[size];
        Arrays.fill(queued, true);
        NodeQueue queue = new NodeQueue(nodes);
        for (int node = 0; node < size; node++) queue.add(node);
        while (!queue.isEmpty()) {
            int node = queue.remove();
            queued[node] = false;
            for (int index = offsets[node]; index < offsets[node + 1]; index++) {
                int arc = arcs[index];
                if (residual(arc) == 0) continue;
                long candidate = distance[node] + cost(arc, objective, 0) + price[node] - price[destination(arc)];
                if (candidate < distance[destination(arc)]) {
                    distance[destination(arc)] = candidate;
                    if (!queued[destination(arc)]) {
                        queued[destination(arc)] = true;
                        queue.add(destination(arc));
                    }
                }
            }
        }
        for (int node = 0; node < size; node++) price[node] += distance[node];
        for (int edge = 0; edge < edges; edge++) {
            long difference = price[from[edge]] - price[to[edge]];
            int forward = admissible(2 * edge, objective, 1, difference, true);
            int reverse = admissible(2 * edge + 1, objective, 1, -difference, true);
            upper[edge] = flow[edge] + forward;
            lower[edge] = flow[edge] - reverse;
        }
    }
}
