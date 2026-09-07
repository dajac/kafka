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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Uniform2AssignorTest {
    private static final class Fixture implements GroupSpec, SubscribedTopicDescriber {
        final int memberCount;
        final int[] partitions;
        final boolean[][] subscriptions;
        final String[] racks;
        final List<List<Set<String>>> replicas = new ArrayList<>();
        final Map<String, MemberAssignment> previous = new HashMap<>();
        boolean forbidRackAccess;
        boolean reverse;

        Fixture(int memberCount, int... partitions) {
            this.memberCount = memberCount;
            this.partitions = partitions;
            this.subscriptions = new boolean[memberCount][partitions.length];
            this.racks = new String[memberCount];
            Arrays.fill(racks, "r0");
            for (boolean[] subscription : subscriptions) Arrays.fill(subscription, true);
            for (int count : partitions) {
                List<Set<String>> topic = new ArrayList<>();
                for (int partition = 0; partition < count; partition++) topic.add(Set.of());
                replicas.add(topic);
            }
        }

        Uuid topic(int index) {
            return new Uuid(0, index + 1);
        }

        @Override
        public Collection<String> memberIds() {
            List<String> result = new ArrayList<>();
            for (int member = 0; member < memberCount; member++) result.add("m" + member);
            if (reverse) Collections.reverse(result);
            return result;
        }

        @Override
        public SubscriptionType subscriptionType() {
            return SubscriptionType.HETEROGENEOUS;
        }

        @Override
        public boolean isPartitionAssigned(Uuid topicId, int partition) {
            throw new AssertionError("Assignment must come from member ownership");
        }

        @Override
        public boolean isPartitionAssignable(Uuid topicId, int partition) {
            return true;
        }

        @Override
        public MemberSubscription memberSubscription(String memberId) {
            int member = Integer.parseInt(memberId.substring(1));
            return new MemberSubscription() {
                @Override
                public Optional<String> rackId() {
                    if (forbidRackAccess) throw new AssertionError("Rack access disabled");
                    return Optional.ofNullable(racks[member]);
                }

                @Override
                public Optional<String> instanceId() {
                    return Optional.empty();
                }

                @Override
                public Set<Uuid> subscribedTopicIds() {
                    Set<Uuid> result = new HashSet<>();
                    for (int topic = 0; topic < partitions.length; topic++) {
                        if (subscriptions[member][topic]) result.add(topic(topic));
                    }
                    return result;
                }
            };
        }

        @Override
        public MemberAssignment memberAssignment(String memberId) {
            return previous.getOrDefault(memberId, new MemberAssignmentImpl(Map.of()));
        }

        @Override
        public int numPartitions(Uuid topicId) {
            int topic = (int) topicId.getLeastSignificantBits() - 1;
            return topic < partitions.length ? partitions[topic] : -1;
        }

        @Override
        public Set<String> racksForPartition(Uuid topicId, int partition) {
            if (forbidRackAccess) throw new AssertionError("Replica rack access disabled");
            return replicas.get((int) topicId.getLeastSignificantBits() - 1).get(partition);
        }

        GroupAssignment assignment(int[][] owners) {
            Map<String, MemberAssignment> result = new LinkedHashMap<>();
            for (int member = 0; member < memberCount; member++) {
                Map<Uuid, Set<Integer>> assigned = new HashMap<>();
                for (int topic = 0; topic < partitions.length; topic++) {
                    for (int partition = 0; partition < partitions[topic]; partition++) {
                        if (owners[topic][partition] == member) {
                            assigned.computeIfAbsent(topic(topic), __ -> new HashSet<>()).add(partition);
                        }
                    }
                }
                result.put("m" + member, new MemberAssignmentImpl(assigned));
            }
            return new GroupAssignment(result);
        }

        int[][] owners(GroupAssignment assignment) {
            int[][] owners = new int[partitions.length][];
            for (int topic = 0; topic < partitions.length; topic++) {
                owners[topic] = new int[partitions[topic]];
                Arrays.fill(owners[topic], -1);
            }
            assignment.members().forEach((id, assigned) -> {
                int member = Integer.parseInt(id.substring(1));
                assertTrue(member < memberCount);
                assigned.partitions().forEach((uuid, values) -> {
                    int topic = (int) uuid.getLeastSignificantBits() - 1;
                    assertTrue(subscriptions[member][topic]);
                    for (int partition : values) {
                        assertTrue(partition >= 0 && partition < partitions[topic]);
                        assertEquals(-1, owners[topic][partition], "Duplicate owner");
                        owners[topic][partition] = member;
                    }
                });
            });
            for (int topic = 0; topic < partitions.length; topic++) {
                boolean subscribed = false;
                for (boolean[] subscription : subscriptions) subscribed |= subscription[topic];
                for (int owner : owners[topic]) assertEquals(subscribed, owner >= 0, "Coverage");
            }
            return owners;
        }

        long[] score(int[][] owners, boolean rackAware) {
            long[] score = new long[4];
            int[] loads = new int[memberCount];
            for (int topic = 0; topic < partitions.length; topic++) {
                int[] counts = new int[memberCount];
                for (int partition = 0; partition < partitions[topic]; partition++) {
                    int owner = owners[topic][partition];
                    if (owner < 0) continue;
                    loads[owner]++;
                    counts[owner]++;
                    Set<String> available = replicas.get(topic).get(partition);
                    if (rackAware && !available.isEmpty() && !available.contains(racks[owner])) score[2]++;
                    for (int member = 0; member < memberCount; member++) {
                        if (member != owner && memberAssignment("m" + member).partitions()
                            .getOrDefault(topic(topic), Set.of()).contains(partition)) score[3]++;
                    }
                }
                for (int count : counts) score[1] += (long) count * count;
            }
            for (int load : loads) score[0] += (long) load * load;
            return score;
        }

        long[] exhaustive(int[][] owners, int topic, int partition, boolean rackAware) {
            if (topic == partitions.length) return score(owners, rackAware);
            if (partition == partitions[topic]) return exhaustive(owners, topic + 1, 0, rackAware);
            long[] best = null;
            for (int member = 0; member < memberCount; member++) {
                if (subscriptions[member][topic]) {
                    owners[topic][partition] = member;
                    long[] candidate = exhaustive(owners, topic, partition + 1, rackAware);
                    if (best == null || Arrays.compare(candidate, best) < 0) best = candidate;
                }
            }
            if (best == null) {
                owners[topic][partition] = -1;
                best = exhaustive(owners, topic, partition + 1, rackAware);
            }
            return best;
        }
    }

    private Uniform2Assignor assignor(boolean rackAware) {
        Uniform2Assignor assignor = new Uniform2Assignor();
        assignor.configure(Map.of(Uniform2Assignor.RACK_AWARE_CONFIG, rackAware));
        return assignor;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fuzzAgainstExhaustiveOracle(boolean rackAware) {
        Uniform2Assignor assignor = assignor(rackAware);
        int trials = Integer.getInteger("uniform2.fuzz.trials", 1000);
        for (int seed = 0; seed < trials; seed++) {
            Random random = new Random(seed);
            Fixture fixture = new Fixture(1 + random.nextInt(3), random.nextInt(4), random.nextInt(4), random.nextInt(3));
            int[][] old = new int[fixture.partitions.length][];
            for (int member = 0; member < fixture.memberCount; member++) {
                fixture.racks[member] = "r" + random.nextInt(3);
                for (int topic = 0; topic < fixture.partitions.length; topic++) {
                    fixture.subscriptions[member][topic] = random.nextBoolean();
                }
            }
            for (int topic = 0; topic < old.length; topic++) {
                old[topic] = new int[fixture.partitions[topic]];
                for (int partition = 0; partition < old[topic].length; partition++) {
                    old[topic][partition] = random.nextInt(fixture.memberCount + 1) - 1;
                    Set<String> replicas = new HashSet<>();
                    for (int rack = 0; rack < 3; rack++) {
                        if (random.nextBoolean()) replicas.add("r" + rack);
                    }
                    fixture.replicas.get(topic).set(partition, replicas);
                }
            }
            fixture.previous.putAll(fixture.assignment(old).members());
            fixture.forbidRackAccess = !rackAware;
            GroupAssignment result = assignor.assign(fixture, fixture);
            int[][] owners = fixture.owners(result);
            long[] actual = fixture.score(owners, rackAware);
            long[] expected = fixture.exhaustive(owners, 0, 0, rackAware);
            assertArrayEquals(expected, actual, "seed=" + seed + ", racks=" + rackAware);
            fixture.reverse = true;
            assertEquals(result, assignor.assign(fixture, fixture), "Iteration order, seed=" + seed);
            fixture.previous.clear();
            fixture.previous.putAll(result.members());
            assertEquals(result, assignor.assign(fixture, fixture), "Fixed point, seed=" + seed);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fuzzTopologyAndMetadataChanges(boolean rackAware) {
        Uniform2Assignor assignor = assignor(rackAware);
        for (int seed = 0; seed < 100; seed++) {
            Random random = new Random(seed);
            GroupAssignment previous = new GroupAssignment(Map.of());
            for (int round = 0; round < 12; round++) {
                int[] sizes = new int[1 + random.nextInt(5)];
                for (int topic = 0; topic < sizes.length; topic++) sizes[topic] = random.nextInt(25);
                Fixture fixture = new Fixture(1 + random.nextInt(12), sizes);
                boolean homogeneous = random.nextBoolean();
                for (int member = 0; member < fixture.memberCount; member++) {
                    fixture.racks[member] = "r" + random.nextInt(3);
                    for (int topic = 0; topic < sizes.length; topic++) {
                        fixture.subscriptions[member][topic] = homogeneous || random.nextBoolean();
                    }
                }
                for (List<Set<String>> replicas : fixture.replicas) {
                    for (int partition = 0; partition < replicas.size(); partition++) {
                        int rack = random.nextInt(3);
                        replicas.set(partition, Set.of("r" + rack, "r" + (rack + 1) % 3));
                    }
                }
                fixture.previous.putAll(previous.members());
                GroupAssignment result = assignor.assign(fixture, fixture);
                int[][] owners = fixture.owners(result);
                if (homogeneous) {
                    int[] loads = new int[fixture.memberCount];
                    for (int[] topic : owners) {
                        int[] counts = new int[fixture.memberCount];
                        for (int owner : topic) {
                            counts[owner]++;
                            loads[owner]++;
                        }
                        assertTrue(Arrays.stream(counts).max().orElse(0) - Arrays.stream(counts).min().orElse(0) <= 1);
                    }
                    assertTrue(Arrays.stream(loads).max().orElse(0) - Arrays.stream(loads).min().orElse(0) <= 1);
                }
                fixture.previous.clear();
                fixture.previous.putAll(result.members());
                assertEquals(result, assignor.assign(fixture, fixture), "seed=" + seed + ", round=" + round);
                previous = result;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void balancesThroughLongSubscriptionChain(boolean rackAware) {
        int members = 512;
        int[] sizes = new int[members];
        Arrays.fill(sizes, 1);
        Fixture fixture = new Fixture(members, sizes);
        for (boolean[] subscriptions : fixture.subscriptions) Arrays.fill(subscriptions, false);
        int[][] previous = new int[members][1];
        for (int topic = 0; topic < members - 1; topic++) {
            fixture.subscriptions[topic][topic] = true;
            fixture.subscriptions[topic + 1][topic] = true;
            previous[topic][0] = topic;
        }
        fixture.subscriptions[0][members - 1] = true;
        fixture.previous.putAll(fixture.assignment(previous).members());
        GroupAssignment result = assignor(rackAware).assign(fixture, fixture);
        assertArrayEquals(new long[] {members, members, 0, members - 1},
            fixture.score(fixture.owners(result), rackAware));
    }

    @Test
    void rackAwarenessRequiresEveryMemberRack() {
        Fixture fixture = new Fixture(3, 6);
        fixture.racks[2] = null;
        Uniform2Assignor enabled = assignor(true);
        GroupAssignment expected = assignor(false).assign(fixture, fixture);
        SubscribedTopicDescriber noRacks = new SubscribedTopicDescriber() {
            @Override
            public int numPartitions(Uuid topicId) {
                return fixture.numPartitions(topicId);
            }

            @Override
            public Set<String> racksForPartition(Uuid topicId, int partition) {
                throw new AssertionError("Partial member racks must disable replica lookup");
            }
        };
        assertEquals(expected, enabled.assign(fixture, noRacks));
        fixture.racks[2] = "";
        assertEquals(expected, enabled.assign(fixture, noRacks));
    }

    @Test
    void realignsReplicationFactorTwoAcrossThreeRacks() {
        Fixture fixture = new Fixture(3, 6, 6);
        for (int member = 0; member < 3; member++) fixture.racks[member] = "r" + member;
        Uniform2Assignor assignor = assignor(true);
        GroupAssignment result = assignor.assign(fixture, fixture);
        for (int round = 0; round < 6; round++) {
            fixture.previous.clear();
            fixture.previous.putAll(result.members());
            for (int topic = 0; topic < 2; topic++) {
                for (int partition = 0; partition < 6; partition++) {
                    int rack = (partition + round) % 3;
                    fixture.replicas.get(topic).set(partition, Set.of("r" + rack, "r" + (rack + 1) % 3));
                }
            }
            result = assignor.assign(fixture, fixture);
            long[] score = fixture.score(fixture.owners(result), true);
            assertEquals(48, score[0]);
            assertEquals(24, score[1]);
            assertEquals(0, score[2]);
        }
    }

    @Test
    void spreadsPreviouslyConcentratedTopicsWithMinimumMovement() {
        Fixture fixture = new Fixture(2, 4, 4);
        fixture.previous.putAll(fixture.assignment(new int[][] {{0, 0, 0, 0}, {1, 1, 1, 1}}).members());
        GroupAssignment result = assignor(false).assign(fixture, fixture);
        assertArrayEquals(new long[] {32, 16, 0, 4}, fixture.score(fixture.owners(result), false));
    }

    @Test
    void handlesEmptyGroupsAndMissingTopics() {
        assertTrue(assignor(false).assign(new Fixture(0, 3), new Fixture(0, 3)).members().isEmpty());
        Fixture fixture = new Fixture(1, 1);
        SubscribedTopicDescriber missing = new Fixture(1);
        assertThrows(PartitionAssignorException.class, () -> assignor(false).assign(fixture, missing));
        assertTrue(assignor(false).assign(new Fixture(1), new Fixture(1)).members().get("m0").partitions().isEmpty());
    }

    @Test
    void validatesConfigurationAndDefaultsToDisabled() {
        Uniform2Assignor assignor = new Uniform2Assignor();
        assertEquals("uniform2", assignor.name());
        assertThrows(ConfigException.class, () -> assignor.configure(Map.of(Uniform2Assignor.RACK_AWARE_CONFIG, "invalid")));
        assignor.configure(Map.of(Uniform2Assignor.RACK_AWARE_CONFIG, "true"));
        assignor.configure(Map.of());
        Fixture fixture = new Fixture(2, 3);
        fixture.forbidRackAccess = true;
        fixture.owners(assignor.assign(fixture, fixture));
    }
}
