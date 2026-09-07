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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Incremental assignment with lexicographic residual-cycle optimization. */
public class Uniform3Assignor implements ConsumerGroupPartitionAssignor, Configurable {
    public static final String NAME = "uniform3";
    public static final String RACK_AWARE_CONFIG = "group.consumer.uniform3.rack.aware.enable";
    public static final String RACK_AWARE_DOC = "Enable rack-aware assignment for uniform3 when every member has a nonempty rack. " +
        "Rack locality is optimized after total and per-topic balance and before stickiness.";
    private boolean rackAware;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object value = configs.get(RACK_AWARE_CONFIG);
        rackAware = value != null && (Boolean) ConfigDef.parseType(RACK_AWARE_CONFIG, value, ConfigDef.Type.BOOLEAN);
    }

    public boolean rackAwareEnabled() {
        return rackAware;
    }

    @Override
    public GroupAssignment assign(GroupSpec spec, SubscribedTopicDescriber describer) {
        List<String> members = new ArrayList<>(spec.memberIds());
        members.sort(String::compareTo);
        if (members.isEmpty()) return new GroupAssignment(Map.of());
        String[] racks = null;
        if (rackAware) {
            racks = new String[members.size()];
            for (int m = 0; m < members.size(); m++) {
                racks[m] = spec.memberSubscription(members.get(m)).rackId().orElse("");
                if (racks[m].isEmpty()) {
                    racks = null;
                    break;
                }
            }
        }
        return new Repair(spec, describer, members, racks).build();
    }

    private static final class Topic {
        final Uuid id;
        final int[] owners;
        final int[] original;
        int[] subscribers = new int[4];
        int size;
        int[] counts;
        int low;
        int high;
        List<Set<String>> replicas;
        int[] edges;
        List<RackGroup> groups;

        Topic(Uuid id, int partitions) {
            this.id = id;
            owners = new int[partitions];
            Arrays.fill(owners, -1);
            original = new int[partitions];
        }

        int subscribe(int member) {
            if (size == subscribers.length) subscribers = Arrays.copyOf(subscribers, size * 2);
            subscribers[size] = member;
            return size++;
        }
    }

    private static final class RackGroup {
        final Set<String> racks;
        int[] pending = new int[4];
        int size;
        int[] owners;
        int[] starts;
        int[] retained;
        int[] partitions;
        int[] destinations;

        RackGroup(Set<String> racks) {
            this.racks = racks;
        }

        void add(int partition) {
            if (size == pending.length) pending = Arrays.copyOf(pending, size * 2);
            pending[size++] = partition;
        }

        int remote(String rack) {
            return racks.isEmpty() || racks.contains(rack) ? 0 : 1;
        }
    }

    private static final class Repair {
        final GroupSpec spec;
        final SubscribedTopicDescriber describer;
        final List<String> members;
        final String[] racks;
        final Map<Uuid, Topic> topics = new TreeMap<>();
        final MemberAssignment[] previous;
        final int[] loads;
        int total;
        int low;
        int high;
        int movementBound;

        Repair(GroupSpec spec, SubscribedTopicDescriber describer, List<String> members, String[] racks) {
            this.spec = spec;
            this.describer = describer;
            this.members = members;
            this.racks = racks;
            previous = new MemberAssignment[members.size()];
            loads = new int[members.size()];
        }

        GroupAssignment build() {
            read();
            low = total / members.size();
            high = low + (total % members.size() == 0 ? 0 : 1);
            bound();
            if (certified()) return materialize();
            releaseMandatory();
            if (placeReleased()) {
                repairDirect();
                if (certified()) return materialize();
            }
            return optimizeResidual();
        }

        void releaseMandatory() {
            // Remove mandatory topic excess and known remote ownership first.
            for (Topic topic : topics.values()) {
                for (int p = 0; p < topic.owners.length; p++) {
                    int owner = topic.owners[p];
                    if (owner >= 0 && !local(topic, p, owner)) remove(topic, p);
                }
                for (int p = 0; p < topic.owners.length; p++) {
                    int owner = topic.owners[p];
                    if (owner >= 0 && topic.counts[owner] > topic.high) remove(topic, p);
                }
            }
        }

        boolean placeReleased() {
            // Place orphaned/new/released partitions without exceeding either ceiling.
            for (Topic topic : topics.values()) {
                Candidates candidates = new Candidates(topic);
                for (int p = 0; p < topic.owners.length; p++) {
                    if (topic.owners[p] >= 0) continue;
                    int target = candidates.take(p, false);
                    if (target < 0) return false;
                    give(topic, p, target);
                    candidates.offer(target);
                }
            }
            return true;
        }

        void repairDirect() {
            // Repair topic deficits and total excess directly. Transfers through other
            // members are resolved by residual cycles in the next phase.
            for (Topic topic : topics.values()) {
                Candidates candidates = new Candidates(topic);
                int deficit = 0;
                for (int count : topic.counts) deficit += Math.max(0, topic.low - count);
                for (int p = 0; p < topic.owners.length; p++) {
                    int owner = topic.owners[p];
                    int member = topic.subscribers[owner];
                    if (topic.counts[owner] <= topic.low || loads[member] <= low) continue;
                    boolean totalExcess = loads[member] > high;
                    if (!totalExcess && deficit == 0) continue;
                    int target = candidates.take(p, !totalExcess);
                    if (target < 0) continue;
                    if (topic.counts[target] < topic.low) deficit--;
                    remove(topic, p);
                    give(topic, p, target);
                    candidates.offer(target);
                    candidates.offer(owner);
                }
            }
        }

        boolean certified() {
            int movements = 0;
            for (int load : loads) if (load < low || load > high) return false;
            for (Topic topic : topics.values()) {
                for (int count : topic.counts) if (count < topic.low || count > topic.high) return false;
                for (int p = 0; p < topic.owners.length; p++) {
                    if (topic.owners[p] < 0 || !local(topic, p, topic.owners[p])) return false;
                    if (topic.original[p] >= 0 && topic.original[p] != topic.owners[p]) movements++;
                }
            }
            if (movements != movementBound) return false;
            return true;
        }

        void read() {
            for (int m = 0; m < members.size(); m++) {
                String id = members.get(m);
                previous[m] = spec.memberAssignment(id);
                for (Uuid topicId : spec.memberSubscription(id).subscribedTopicIds()) {
                    Topic topic = topics.get(topicId);
                    if (topic == null) {
                        int count = describer.numPartitions(topicId);
                        if (count < 0) throw new PartitionAssignorException("Subscribed topic " + topicId + " does not exist");
                        topic = new Topic(topicId, count);
                        total = Math.addExact(total, count);
                        topics.put(topicId, topic);
                    }
                    int subscriber = topic.subscribe(m);
                    for (int p : previous[m].partitions().getOrDefault(topicId, Set.of())) {
                        if (p >= 0 && p < topic.owners.length && topic.owners[p] < 0) topic.owners[p] = subscriber;
                    }
                }
            }
            for (Topic topic : topics.values()) {
                topic.subscribers = Arrays.copyOf(topic.subscribers, topic.size);
                topic.counts = new int[topic.size];
                topic.low = topic.owners.length / topic.size;
                topic.high = topic.low + (topic.owners.length % topic.size == 0 ? 0 : 1);
                System.arraycopy(topic.owners, 0, topic.original, 0, topic.owners.length);
                for (int owner : topic.owners) {
                    if (owner >= 0) {
                        topic.counts[owner]++;
                        loads[topic.subscribers[owner]]++;
                    }
                }
                if (racks != null) {
                    topic.replicas = new ArrayList<>(topic.owners.length);
                    for (int p = 0; p < topic.owners.length; p++) topic.replicas.add(describer.racksForPartition(topic.id, p));
                }
            }
        }

        /** Independent necessary movement bounds; take their maximum, not their sum. */
        void bound() {
            int unowned = 0;
            int topicBound = 0;
            int remote = 0;
            for (Topic topic : topics.values()) {
                int missing = 0;
                for (int p = 0; p < topic.owners.length; p++) {
                    if (topic.owners[p] < 0) missing++;
                    else if (!local(topic, p, topic.owners[p])) remote++;
                }
                int excess = 0;
                int deficit = 0;
                for (int count : topic.counts) {
                    excess += Math.max(0, count - topic.high);
                    deficit += Math.max(0, topic.low - count);
                }
                topicBound += Math.max(excess, deficit - missing);
                unowned += missing;
            }
            int excess = 0;
            int deficit = 0;
            for (int load : loads) {
                excess += Math.max(0, load - high);
                deficit += Math.max(0, low - load);
            }
            movementBound = Math.max(Math.max(excess, deficit - unowned), Math.max(topicBound, remote));
        }

        boolean local(Topic topic, int partition, int subscriber) {
            return racks == null || topic.replicas.get(partition).isEmpty() ||
                topic.replicas.get(partition).contains(racks[topic.subscribers[subscriber]]);
        }

        void remove(Topic topic, int partition) {
            int owner = topic.owners[partition];
            topic.counts[owner]--;
            loads[topic.subscribers[owner]]--;
            topic.owners[partition] = -1;
        }

        void give(Topic topic, int partition, int subscriber) {
            topic.owners[partition] = subscriber;
            topic.counts[subscriber]++;
            loads[topic.subscribers[subscriber]]++;
        }

        // Each subscriber belongs to one heap. Only popped recipients change keys;
        // donors are at/above the total ceiling and therefore are not in any heap.
        final class Candidates {
            final Topic topic;
            final Map<String, Heap> byRack;
            final Heap plain;
            final Heap[] memberHeap;

            Candidates(Topic topic) {
                this.topic = topic;
                if (racks == null) {
                    byRack = null;
                    memberHeap = null;
                    plain = new Heap(topic.size);
                    for (int i = 0; i < topic.size; i++) offer(i);
                    return;
                }
                byRack = new TreeMap<>();
                plain = null;
                memberHeap = new Heap[topic.size];
                Map<String, Integer> sizes = new TreeMap<>();
                for (int member : topic.subscribers) sizes.merge(racks[member], 1, Integer::sum);
                sizes.forEach((rack, size) -> byRack.put(rack, new Heap(size)));
                for (int i = 0; i < topic.size; i++) {
                    memberHeap[i] = byRack.get(racks[topic.subscribers[i]]);
                    offer(i);
                }
            }

            boolean before(int a, int b) {
                boolean aNeedsTopic = topic.counts[a] < topic.low;
                boolean bNeedsTopic = topic.counts[b] < topic.low;
                if (aNeedsTopic != bNeedsTopic) return aNeedsTopic;
                int aLoad = loads[topic.subscribers[a]];
                int bLoad = loads[topic.subscribers[b]];
                return aLoad < bLoad || (aLoad == bLoad && a < b);
            }

            void offer(int subscriber) {
                if (topic.counts[subscriber] < topic.high && loads[topic.subscribers[subscriber]] < high) (racks == null ? plain : memberHeap[subscriber]).add(subscriber);
            }

            int take(int partition, boolean requireTopicDeficit) {
                if (racks == null) {
                    if (plain.size == 0 || (requireTopicDeficit && topic.counts[plain.values[0]] >= topic.low)) return -1;
                    return plain.remove();
                }
                return takeLocal(partition, requireTopicDeficit);
            }

            int takeLocal(int partition, boolean requireTopicDeficit) {
                Heap best = null;
                for (Map.Entry<String, Heap> entry : byRack.entrySet()) {
                    Heap heap = entry.getValue();
                    if (heap.size == 0) continue;
                    if (!topic.replicas.get(partition).isEmpty() && !topic.replicas.get(partition).contains(entry.getKey())) continue;
                    if (requireTopicDeficit && topic.counts[heap.values[0]] >= topic.low) continue;
                    if (best == null || before(heap.values[0], best.values[0])) best = heap;
                }
                return best == null ? -1 : best.remove();
            }

            final class Heap {
                final int[] values;
                int size;

                Heap(int capacity) {
                    values = new int[capacity];
                }

                void add(int subscriber) {
                    int index = size++;
                    while (index > 0) {
                        int parent = (index - 1) >>> 1;
                        if (!before(subscriber, values[parent])) break;
                        values[index] = values[parent];
                        index = parent;
                    }
                    values[index] = subscriber;
                }

                int remove() {
                    int result = values[0];
                    int last = values[--size];
                    int index = 0;
                    while (index < size / 2) {
                        int child = 2 * index + 1;
                        if (child + 1 < size && before(values[child + 1], values[child])) child++;
                        if (!before(values[child], last)) break;
                        values[index] = values[child];
                        index = child;
                    }
                    if (size > 0) values[index] = last;
                    return result;
                }
            }
        }

        GroupAssignment optimizeResidual() {
            completeSeed();
            int[] capacity = graphSize();
            Uniform3AssignmentGraph graph = new Uniform3AssignmentGraph(capacity[0], capacity[1]);
            int sink = graph.node();
            int[] memberNodes = new int[members.size()];
            for (int m = 0; m < members.size(); m++) {
                memberNodes[m] = graph.node();
                graph.edge(memberNodes[m], sink, total, loads[m], 0, 0, total);
            }
            for (Topic topic : topics.values()) buildTopicGraph(topic, graph, memberNodes);
            graph.optimize();
            for (Topic topic : topics.values()) {
                for (int i = 0; i < topic.size; i++) topic.counts[i] = graph.flow[topic.edges[i]];
                if (racks == null) assignCounts(topic);
                else assignRackCounts(topic, graph);
            }
            return materialize();
        }

        void completeSeed() {
            // Complete a feasible starting assignment before optimizing cycles. Existing
            // valid ownership and every successful direct repair remain in the seed.
            for (Topic topic : topics.values()) {
                for (int p = 0; p < topic.owners.length; p++) {
                    if (topic.owners[p] >= 0) continue;
                    int best = 0;
                    for (int i = 1; i < topic.size; i++) {
                        if (topic.counts[i] < topic.counts[best] ||
                            (topic.counts[i] == topic.counts[best] && loads[topic.subscribers[i]] < loads[topic.subscribers[best]])) best = i;
                    }
                    give(topic, p, best);
                }
            }
        }

        int[] graphSize() {
            int nodes = members.size() + 1;
            int edges = members.size();
            for (Topic topic : topics.values()) {
                nodes = Math.addExact(nodes, racks == null ? 1 : topic.size);
                edges = Math.addExact(edges, topic.size);
                if (racks != null) {
                    prepareGroups(topic);
                    for (RackGroup group : topic.groups) {
                        nodes = Math.addExact(nodes, group.owners.length + 1);
                        edges = Math.addExact(edges, topic.size + group.owners.length);
                        for (int owner : group.owners) if (owner >= 0) edges = Math.addExact(edges, 1);
                    }
                }
            }
            return new int[] {nodes, edges};
        }

        void buildTopicGraph(Topic topic, Uniform3AssignmentGraph graph, int[] memberNodes) {
            topic.edges = new int[topic.size];
            if (racks == null) {
                int source = graph.node();
                int[] oldCounts = new int[topic.size];
                for (int owner : topic.original) if (owner >= 0) oldCounts[owner]++;
                for (int i = 0; i < topic.size; i++) topic.edges[i] = graph.edge(source, memberNodes[topic.subscribers[i]],
                    topic.owners.length, topic.counts[i], 1, 0, oldCounts[i]);
            } else {
                int[] destinations = new int[topic.size];
                for (int i = 0; i < topic.size; i++) {
                    destinations[i] = graph.node();
                    topic.edges[i] = graph.edge(destinations[i], memberNodes[topic.subscribers[i]],
                        topic.owners.length, topic.counts[i], 1, 0, topic.owners.length);
                }
                for (RackGroup group : topic.groups) {
                    int hub = graph.node();
                    int[] released = new int[topic.size];
                    for (int b = 0; b < group.owners.length; b++) {
                        int source = graph.node();
                        int owner = group.owners[b];
                        int kept = 0;
                        int count = group.starts[b + 1] - group.starts[b];
                        for (int i = group.starts[b]; i < group.starts[b + 1]; i++) {
                            int actual = topic.owners[group.partitions[i]];
                            if (actual == owner) kept++;
                            else released[actual]++;
                        }
                        if (owner >= 0) group.retained[b] = graph.edge(source, destinations[owner], count, kept, -1,
                            group.remote(racks[topic.subscribers[owner]]), count);
                        graph.edge(source, hub, count, count - kept, -1, 0, owner < 0 ? count : 0);
                    }
                    group.destinations = new int[topic.size];
                    for (int i = 0; i < topic.size; i++) group.destinations[i] = graph.edge(hub, destinations[i], topic.owners.length,
                        released[i], -1, group.remote(racks[topic.subscribers[i]]), topic.owners.length);
                }
            }
        }

        void assignCounts(Topic topic) {
            int[] remaining = Arrays.copyOf(topic.counts, topic.size);
            Arrays.fill(topic.owners, -1);
            for (int p = 0; p < topic.owners.length; p++) {
                int owner = topic.original[p];
                if (owner >= 0 && remaining[owner] > 0) {
                    topic.owners[p] = owner;
                    remaining[owner]--;
                }
            }
            int next = 0;
            for (int p = 0; p < topic.owners.length; p++) {
                if (topic.owners[p] >= 0) continue;
                while (remaining[next] == 0) next++;
                topic.owners[p] = next;
                remaining[next]--;
            }
        }


        void assignRackCounts(Topic topic, Uniform3AssignmentGraph graph) {
            for (RackGroup group : topic.groups) {
                int[] pool = new int[group.partitions.length];
                int size = 0;
                for (int b = 0; b < group.owners.length; b++) {
                    int retained = group.retained[b] < 0 ? 0 : graph.flow[group.retained[b]];
                    for (int i = group.starts[b]; i < group.starts[b + 1]; i++) {
                        int p = group.partitions[i];
                        if (i - group.starts[b] < retained) topic.owners[p] = group.owners[b];
                        else pool[size++] = p;
                    }
                }
                int cursor = 0;
                for (int i = 0; i < topic.size; i++) {
                    for (int n = 0; n < graph.flow[group.destinations[i]]; n++) topic.owners[pool[cursor++]] = i;
                }
            }
        }


        void prepareGroups(Topic topic) {
            Map<Set<String>, RackGroup> groups = new LinkedHashMap<>();
            for (int p = 0; p < topic.owners.length; p++) {
                Set<String> replicas = topic.replicas.get(p);
                groups.computeIfAbsent(replicas, RackGroup::new).add(p);
            }
            topic.groups = new ArrayList<>(groups.values());
            for (RackGroup group : topic.groups) {
                int[] counts = new int[topic.size + 1];
                for (int i = 0; i < group.size; i++) counts[topic.original[group.pending[i]] + 1]++;
                int buckets = 0;
                for (int count : counts) if (count > 0) buckets++;
                group.owners = new int[buckets];
                group.starts = new int[buckets + 1];
                group.retained = new int[buckets];
                Arrays.fill(group.retained, -1);
                int b = 0;
                for (int i = 0; i < counts.length; i++) {
                    if (counts[i] == 0) continue;
                    group.owners[b] = i - 1;
                    group.starts[b + 1] = group.starts[b] + counts[i];
                    counts[i] = group.starts[b++];
                }
                group.partitions = new int[group.size];
                for (int i = 0; i < group.size; i++) {
                    int p = group.pending[i];
                    group.partitions[counts[topic.original[p] + 1]++] = p;
                }
                group.pending = null;
            }
        }

        GroupAssignment materialize() {
            List<Map<Uuid, Set<Integer>>> assignments = new ArrayList<>();
            for (int m = 0; m < members.size(); m++) assignments.add(new HashMap<>());
            for (Topic topic : topics.values()) {
                int[] starts = new int[topic.size + 1];
                for (int i = 0; i < topic.size; i++) starts[i + 1] = starts[i] + topic.counts[i];
                int[] cursor = Arrays.copyOf(starts, topic.size);
                int[] partitions = new int[topic.owners.length];
                for (int p = 0; p < topic.owners.length; p++) partitions[cursor[topic.owners[p]]++] = p;
                for (int i = 0; i < topic.size; i++) {
                    if (starts[i] < starts[i + 1]) assignments.get(topic.subscribers[i]).put(topic.id,
                        Uniform2PartitionSet.of(partitions, starts[i], starts[i + 1]));
                }
            }
            Map<String, MemberAssignment> result = new LinkedHashMap<>();
            for (int m = 0; m < members.size(); m++) result.put(members.get(m), previous[m].partitions().equals(assignments.get(m)) ?
                previous[m] : new MemberAssignmentImpl(assignments.get(m)));
            return new GroupAssignment(result);
        }
    }
}
