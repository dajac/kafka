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

import static org.apache.kafka.coordinator.group.assignor.Uniform2AssignmentGraph.Objective.RACK;
import static org.apache.kafka.coordinator.group.assignor.Uniform2AssignmentGraph.Objective.TOPIC;
import static org.apache.kafka.coordinator.group.assignor.Uniform2AssignmentGraph.Objective.TOTAL;

/**
 * An experimental uniform assignor with one convex-flow algorithm for all subscriptions.
 * Every topic is spread within floor/ceiling counts over its subscribers. Within those
 * bounds, objectives are minimum sum of squared member loads, minimum remote partitions,
 * and minimum partition movements.
 * Squared loads express balance even when subscriptions make equal loads impossible.
 * Stickiness is exact among assignments attaining the preceding objectives.
 *
 * The rack-free graph has one edge per subscription. With racks, partitions are grouped
 * by replica-rack set and previous owner. No partition/member Cartesian product is built.
 * A new graph is built for each invocation, so topology and replica changes are reevaluated.
 */
public class Uniform2Assignor implements ConsumerGroupPartitionAssignor, Configurable {
    public static final String NAME = "uniform2";
    public static final String RACK_AWARE_CONFIG = "group.consumer.uniform2.rack.aware.enable";
    public static final String RACK_AWARE_DOC = "Enable rack-aware assignment for uniform2 when every member has a nonempty rack. " +
        "Rack locality is optimized after per-topic and total balance and before stickiness.";

    private volatile boolean rackAware = false;

    @Override
    public String name() {
        return NAME;
    }

    public boolean rackAwareEnabled() {
        return rackAware;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object value = configs.get(RACK_AWARE_CONFIG);
        rackAware = value != null && (Boolean) ConfigDef.parseType(RACK_AWARE_CONFIG, value, ConfigDef.Type.BOOLEAN);
    }

    @Override
    public GroupAssignment assign(GroupSpec groupSpec, SubscribedTopicDescriber describer) {
        List<String> members = new ArrayList<>(groupSpec.memberIds());
        members.sort(String::compareTo);
        if (members.isEmpty()) return new GroupAssignment(Map.of());
        return new Builder(groupSpec, describer, members, racks(groupSpec, members)).build();
    }

    private List<String> racks(GroupSpec spec, List<String> members) {
        // Do not even access MemberSubscription.rackId() when the feature is disabled.
        if (!rackAware) return null;
        List<String> racks = new ArrayList<>(members.size());
        for (String member : members) {
            String rack = spec.memberSubscription(member).rackId().orElse("");
            if (rack.isEmpty()) return null;
            racks.add(rack);
        }
        return racks;
    }

    private static final class Topic {
        final Uuid id;
        int[] subscribers = new int[4];
        int subscriberCount;
        final int[] owners;
        int[] counts;
        List<RackGroup> groups;

        Topic(Uuid id, int partitions) {
            this.id = id;
            this.owners = new int[partitions];
            Arrays.fill(owners, -1);
        }

        void addSubscriber(int member) {
            if (subscriberCount == subscribers.length) subscribers = Arrays.copyOf(subscribers, subscriberCount * 2);
            subscribers[subscriberCount++] = member;
        }
    }

    private static final class IntList {
        int[] values = new int[4];
        int size;

        void add(int value) {
            if (size == values.length) values = Arrays.copyOf(values, size * 2);
            values[size++] = value;
        }
    }

    private static final class RackGroup {
        final Set<String> racks;
        IntList pending = new IntList();
        int[] partitions;
        int[] owners;
        int[] starts;
        int[] retained;
        int[] destinations;

        RackGroup(Set<String> racks) {
            this.racks = racks;
        }

        int remote(String rack) {
            // No replica rack metadata means locality is unknown, not a mismatch.
            return racks.isEmpty() || racks.contains(rack) ? 0 : 1;
        }
    }

    private static final class Builder {
        private final GroupSpec spec;
        private final SubscribedTopicDescriber describer;
        private final List<String> members;
        private final List<String> racks;
        private final Map<Uuid, Topic> topics = new TreeMap<>();
        private Uniform2AssignmentGraph graph;
        private final int[] totals;
        private final List<Map<Uuid, Set<Integer>>> assignment = new ArrayList<>();
        private final int[] memberNodes;
        private final int[] memberScratch;
        private final MemberAssignment[] previousAssignments;
        private int[] topicMemberNodes;

        Builder(GroupSpec spec, SubscribedTopicDescriber describer, List<String> members, List<String> racks) {
            this.spec = spec;
            this.describer = describer;
            this.members = members;
            this.racks = racks;
            this.memberNodes = new int[members.size()];
            this.totals = new int[members.size()];
            this.memberScratch = new int[members.size() + 1];
            this.previousAssignments = new MemberAssignment[members.size()];
            for (int member = 0; member < members.size(); member++) previousAssignments[member] = spec.memberAssignment(members.get(member));
        }

        GroupAssignment build() {
            int total = readTopics();
            if (alreadyOptimal(total)) {
                Map<String, MemberAssignment> unchanged = new LinkedHashMap<>();
                for (String member : members) unchanged.put(member, spec.memberAssignment(member));
                return new GroupAssignment(unchanged);
            }
            int subscriptions = topics.values().stream().mapToInt(topic -> topic.subscriberCount).reduce(0, Math::addExact);
            graph = new Uniform2AssignmentGraph(Math.addExact(members.size(), topics.size() + 1),
                Math.addExact(members.size(), subscriptions));
            int sink = graph.node(-total);
            for (int member = 0; member < members.size(); member++) {
                memberNodes[member] = graph.node(0);
                totals[member] = graph.edge(memberNodes[member], sink, total, TOTAL, 0, total);
                assignment.add(new HashMap<>());
            }
            for (Topic topic : topics.values()) addTopic(topic);
            if (!balancedBounds(total)) graph.optimize(TOTAL);
            if (racks != null) optimizeRacks(total);
            if (needsMovementOptimization()) graph.minimizeMovement();
            return materialize();
        }

        private boolean needsMovementOptimization() {
            boolean hasOwners = topics.values().stream()
                .anyMatch(topic -> Arrays.stream(topic.owners).anyMatch(owner -> owner >= 0));
            if (!hasOwners) return false;
            // Fixed topic counts already determine maximum possible retention without
            // racks: materialization keeps min(old count, new count) for every member.
            return racks != null || topics.values().stream()
                .anyMatch(topic -> Arrays.stream(topic.counts).anyMatch(edge -> graph.lower[edge] != graph.upper[edge]));
        }

        private GroupAssignment materialize() {
            for (Topic topic : topics.values()) {
                if (racks == null) assignTopic(topic);
                else assignRackTopic(topic);
                materializeTopic(topic);
            }
            Map<String, MemberAssignment> result = new LinkedHashMap<>();
            for (int member = 0; member < members.size(); member++) {
                MemberAssignment previous = previousAssignments[member];
                result.put(members.get(member), previous.partitions().equals(assignment.get(member)) ?
                    previous : new MemberAssignmentImpl(assignment.get(member)));
            }
            return new GroupAssignment(result);
        }

        private boolean alreadyOptimal(int total) {
            int[] loads = new int[members.size()];
            for (Topic topic : topics.values()) {
                int[] counts = memberScratch;
                Arrays.fill(counts, 0);
                for (int owner : topic.owners) {
                    if (owner < 0) return false;
                    counts[owner]++;
                    loads[owner]++;
                }
                int low = topic.owners.length / topic.subscriberCount;
                int high = low + (topic.owners.length % topic.subscriberCount == 0 ? 0 : 1);
                for (int member : topic.subscribers) {
                    if (counts[member] < low || counts[member] > high) return false;
                }
            }
            int low = total / members.size();
            int high = low + (total % members.size() == 0 ? 0 : 1);
            for (int member = 0; member < members.size(); member++) {
                if (loads[member] < low || loads[member] > high) return false;
                long previous = previousAssignments[member].partitions().values().stream().mapToLong(Set::size).sum();
                if (previous != loads[member]) return false;
            }
            return racks == null || allLocal();
        }

        private boolean allLocal() {
            for (Topic topic : topics.values()) {
                for (int partition = 0; partition < topic.owners.length; partition++) {
                    Set<String> replicas = describer.racksForPartition(topic.id, partition);
                    if (!replicas.isEmpty() && !replicas.contains(racks.get(topic.owners[partition]))) return false;
                }
            }
            return true;
        }

        private void optimizeRacks(int total) {
            Uniform2AssignmentGraph previous = graph;
            topicMemberNodes = new int[members.size()];
            int nodeCount = members.size() + 1;
            int edgeCount = members.size();
            for (Topic topic : topics.values()) {
                prepareRackTopic(topic);
                nodeCount = Math.addExact(nodeCount, topic.subscriberCount);
                edgeCount = Math.addExact(edgeCount, topic.subscriberCount);
                for (RackGroup group : topic.groups) {
                    nodeCount = Math.addExact(nodeCount, group.owners.length + 1);
                    edgeCount = Math.addExact(edgeCount, topic.subscriberCount);
                    for (int owner : group.owners) edgeCount = Math.addExact(edgeCount, owner < 0 ? 1 : 2);
                }
            }
            graph = new Uniform2AssignmentGraph(nodeCount, edgeCount);
            int sink = graph.node(-total);
            for (int member = 0; member < members.size(); member++) {
                memberNodes[member] = graph.node(0);
                copyBounds(previous, totals[member], graph.edge(memberNodes[member], sink, total, null, 0, total));
            }
            for (Topic topic : topics.values()) addRackTopic(topic, previous);
            if (!graph.localFlow()) graph.optimize(RACK);
        }

        /**
         * Topic floor/ceiling bounds are mandatory and jointly feasible: every topic
         * distributes only to its own subscribers. Seed those counts, then certify
         * whether total loads also attain their absolute lower bound. If not, the
         * optimizer balances total loads subject to the fixed topic bounds.
         */
        private boolean balancedBounds(int total) {
            seedBalancedCounts();
            for (Topic topic : topics.values()) {
                int low = topic.owners.length / topic.subscriberCount;
                int high = low + (topic.owners.length % topic.subscriberCount == 0 ? 0 : 1);
                for (int edge : topic.counts) {
                    graph.lower[edge] = low;
                    graph.upper[edge] = high;
                }
            }
            int minimum = total / members.size();
            int maximum = minimum + (total % members.size() == 0 ? 0 : 1);
            for (int edge : totals) {
                if (graph.flow[edge] < minimum || graph.flow[edge] > maximum) return false;
            }
            for (int edge : totals) {
                graph.lower[edge] = minimum;
                graph.upper[edge] = maximum;
            }
            return true;
        }

        private void seedBalancedCounts() {
            long[] loads = new long[members.size()];
            Uniform2AssignmentGraph.NodeHeap candidates = new Uniform2AssignmentGraph.NodeHeap(loads);
            for (Topic topic : topics.values()) {
                int minimum = topic.owners.length / topic.subscriberCount;
                for (int index = 0; index < topic.subscriberCount; index++) {
                    graph.flow[topic.counts[index]] = minimum;
                    graph.flow[totals[topic.subscribers[index]]] += minimum;
                }
            }
            for (Topic topic : topics.values()) {
                int remainder = topic.owners.length % topic.subscriberCount;
                if (remainder == 0) continue;
                if (remainder == 1) {
                    // A singleton (or one leftover partition) needs only the minimum,
                    // not a heap of every subscriber. Preserve the same index tie-break.
                    int best = 0;
                    for (int index = 1; index < topic.subscriberCount; index++) {
                        if (graph.flow[totals[topic.subscribers[index]]] < graph.flow[totals[topic.subscribers[best]]]) best = index;
                    }
                    graph.flow[topic.counts[best]]++;
                    graph.flow[totals[topic.subscribers[best]]]++;
                    continue;
                }
                candidates.clear();
                for (int index = 0; index < topic.subscriberCount; index++) {
                    loads[index] = graph.flow[totals[topic.subscribers[index]]];
                    candidates.addOrDecrease(index);
                }
                for (int count = 0; count < remainder; count++) {
                    int index = candidates.remove();
                    graph.flow[topic.counts[index]]++;
                    graph.flow[totals[topic.subscribers[index]]]++;
                }
            }
        }

        private int readTopics() {
            int total = 0;
            for (int member = 0; member < members.size(); member++) {
                String memberId = members.get(member);
                for (Uuid topicId : spec.memberSubscription(memberId).subscribedTopicIds()) {
                    Topic topic = topics.get(topicId);
                    if (topic == null) {
                        int count = describer.numPartitions(topicId);
                        if (count < 0) throw new PartitionAssignorException("Subscribed topic " + topicId + " does not exist");
                        total = Math.addExact(total, count);
                        topic = new Topic(topicId, count);
                        topics.put(topicId, topic);
                    }
                    topic.addSubscriber(member);
                    for (int partition : previousAssignments[member].partitions().getOrDefault(topicId, Set.of())) {
                        if (partition >= 0 && partition < topic.owners.length && topic.owners[partition] == -1) {
                            topic.owners[partition] = member;
                        }
                    }
                }
            }
            for (Topic topic : topics.values()) topic.subscribers = Arrays.copyOf(topic.subscribers, topic.subscriberCount);
            return total;
        }

        private void addTopic(Topic topic) {
            int source = graph.node(topic.owners.length);
            int[] previousCounts = memberScratch;
            Arrays.fill(previousCounts, 0);
            for (int owner : topic.owners) {
                if (owner >= 0) previousCounts[owner]++;
            }
            topic.counts = new int[topic.subscriberCount];
            for (int index = 0; index < topic.subscriberCount; index++) {
                int member = topic.subscribers[index];
                topic.counts[index] = graph.edge(source, memberNodes[member], topic.owners.length, TOPIC, 0, previousCounts[member]);
            }
        }

        private void prepareRackTopic(Topic topic) {
            Map<Set<String>, RackGroup> groups = new LinkedHashMap<>();
            for (int partition = 0; partition < topic.owners.length; partition++) {
                Set<String> replicas = describer.racksForPartition(topic.id, partition);
                RackGroup group = groups.get(replicas);
                if (group == null) {
                    group = new RackGroup(Set.copyOf(replicas));
                    groups.put(group.racks, group);
                }
                group.pending.add(partition);
            }
            topic.groups = new ArrayList<>(groups.values());
            for (RackGroup group : topic.groups) groupByOwner(topic, group);
        }

        /** Counting scatter retains ascending owner and partition order without boxed buckets. */
        private void groupByOwner(Topic topic, RackGroup group) {
            Arrays.fill(memberScratch, 0);
            int buckets = 0;
            for (int index = 0; index < group.pending.size; index++) {
                int owner = topic.owners[group.pending.values[index]];
                if (memberScratch[owner + 1]++ == 0) buckets++;
            }
            group.owners = new int[buckets];
            group.starts = new int[buckets + 1];
            group.retained = new int[buckets];
            Arrays.fill(group.retained, -1);
            int bucket = 0;
            if (memberScratch[0] != 0) group.owners[bucket++] = -1;
            for (int owner : topic.subscribers) {
                if (memberScratch[owner + 1] != 0) group.owners[bucket++] = owner;
            }
            for (int index = 0; index < buckets; index++) {
                int owner = group.owners[index];
                group.starts[index + 1] = group.starts[index] + memberScratch[owner + 1];
                memberScratch[owner + 1] = group.starts[index];
            }
            group.partitions = new int[group.pending.size];
            for (int index = 0; index < group.pending.size; index++) {
                int partition = group.pending.values[index];
                group.partitions[memberScratch[topic.owners[partition] + 1]++] = partition;
            }
            group.pending = null;
        }

        private void addRackTopic(Topic topic, Uniform2AssignmentGraph previous) {
            for (int index = 0; index < topic.subscriberCount; index++) {
                int member = topic.subscribers[index];
                int node = graph.node(0);
                topicMemberNodes[member] = node;
                copyBounds(previous, topic.counts[index], graph.edge(node, memberNodes[member], topic.owners.length,
                    null, 0, topic.owners.length));
            }
            for (RackGroup group : topic.groups) {
                int hub = graph.node(0);
                for (int bucket = 0; bucket < group.owners.length; bucket++) {
                    int owner = group.owners[bucket];
                    int count = group.starts[bucket + 1] - group.starts[bucket];
                    int source = graph.node(count);
                    if (owner >= 0) {
                        group.retained[bucket] = graph.edge(source, topicMemberNodes[owner], count, null,
                            group.remote(racks.get(owner)), count);
                    }
                    graph.edge(source, hub, count, null, 0, owner < 0 ? count : 0);
                }
                group.destinations = new int[topic.subscriberCount];
                for (int index = 0; index < topic.subscriberCount; index++) {
                    int member = topic.subscribers[index];
                    group.destinations[index] = graph.edge(hub, topicMemberNodes[member], topic.owners.length,
                        null, group.remote(racks.get(member)), topic.owners.length);
                }
            }
        }

        private void copyBounds(Uniform2AssignmentGraph previous, int source, int target) {
            graph.lower[target] = previous.lower[source];
            graph.upper[target] = previous.upper[source];
            graph.flow[target] = previous.lower[source];
        }

        private void assignTopic(Topic topic) {
            int[] remaining = memberScratch;
            Arrays.fill(remaining, 0);
            for (int index = 0; index < topic.subscriberCount; index++) {
                remaining[topic.subscribers[index]] = graph.flow[topic.counts[index]];
            }
            int[] pool = new int[topic.owners.length];
            int poolSize = 0;
            for (int partition = 0; partition < topic.owners.length; partition++) {
                int owner = topic.owners[partition];
                if (owner >= 0 && remaining[owner] > 0) {
                    put(topic, partition, owner);
                    remaining[owner]--;
                } else {
                    pool[poolSize++] = partition;
                }
            }
            int cursor = 0;
            for (int member : topic.subscribers) {
                for (int count = 0; count < remaining[member]; count++) put(topic, pool[cursor++], member);
            }
        }

        private void assignRackTopic(Topic topic) {
            int[] pool = new int[topic.owners.length];
            for (RackGroup group : topic.groups) {
                int poolSize = 0;
                for (int bucket = 0; bucket < group.owners.length; bucket++) {
                    int retain = group.retained[bucket] < 0 ? 0 : graph.flow[group.retained[bucket]];
                    int start = group.starts[bucket];
                    for (int index = start; index < group.starts[bucket + 1]; index++) {
                        int partition = group.partitions[index];
                        if (index - start < retain) put(topic, partition, group.owners[bucket]);
                        else pool[poolSize++] = partition;
                    }
                }
                int cursor = 0;
                for (int index = 0; index < topic.subscriberCount; index++) {
                    for (int count = 0; count < graph.flow[group.destinations[index]]; count++) {
                        put(topic, pool[cursor++], topic.subscribers[index]);
                    }
                }
            }
        }

        private void materializeTopic(Topic topic) {
            Arrays.fill(memberScratch, 0);
            for (int owner : topic.owners) memberScratch[owner + 1]++;
            for (int member = 1; member <= members.size(); member++) memberScratch[member] += memberScratch[member - 1];
            // Member node IDs are no longer needed after solving, so reuse their array
            // as scatter cursors. Partition iteration gives sorted output slices.
            System.arraycopy(memberScratch, 0, memberNodes, 0, members.size());
            int[] partitions = new int[topic.owners.length];
            for (int partition = 0; partition < topic.owners.length; partition++) {
                partitions[memberNodes[topic.owners[partition]]++] = partition;
            }
            for (int member : topic.subscribers) {
                int start = memberScratch[member];
                int end = memberScratch[member + 1];
                if (start != end) assignment.get(member).put(topic.id, Uniform2PartitionSet.of(partitions, start, end));
            }
        }

        private void put(Topic topic, int partition, int member) {
            topic.owners[partition] = member;
        }
    }
}
