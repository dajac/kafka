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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the server side partition assignors of consumer groups.
 *
 * <p>The parameters are independent, so that any combination can be selected with {@code -p}.
 * They describe the group, and what happened to it before the assignment is computed:
 * <ul>
 *     <li>{@code memberCount}: the number of members when the assignment is computed.</li>
 *     <li>{@code topicCount}: the number of subscribed topics.</li>
 *     <li>{@code partitionCount}: the number of partitions over all topics.</li>
 *     <li>{@code topology}: how the partitions are split over the topics. {@code EQUAL} gives
 *     every topic the same number of partitions. {@code SKEWED} gives a few large topics, a band
 *     of small ones and a majority with the smallest size, in geometric tiers, see
 *     {@link Topology#SKEWED}. Every topic has at least one partition.</li>
 *     <li>{@code subscription}: how the members subscribe. {@code HOMOGENEOUS}, every member
 *     subscribes to every topic. {@code DISJOINT}, the members form five buckets, each
 *     subscribing to its own fifth of the topics. {@code NESTED}, five buckets where the members
 *     of bucket {@code b} subscribe to the first {@code b + 1} fifths of the topics, so that
 *     topics are shared by several buckets. Member {@code i} is in bucket {@code i mod 5}, and
 *     the bucket count is capped at the member count and at the topic count.</li>
 *     <li>{@code rack}: {@code NONE}, the members have no rack. {@code PROVIDED}, member
 *     {@code i} is in rack {@code i mod 3}, the racks of the brokers, see {@link Cluster}.
 *     Assignors which do not use racks give the same results for both values.</li>
 *     <li>{@code assignor}: the assignor.</li>
 *     <li>{@code event}: the state of the group. {@code FULL}, no member holds partitions.
 *     {@code STABLE}, the members hold the output of the assignor for the same group, so
 *     nothing has to change. {@code JOIN_ONE} and {@code JOIN_MANY}, one member or a tenth of
 *     them, rounded up, joined and hold nothing. {@code LEAVE_ONE} and {@code LEAVE_MANY}, one
 *     member or a tenth of them left, leaving their partitions unassigned.
 *     {@code PARTITIONS_ADDED}, one topic in ten gained a partition. The group always has
 *     {@code memberCount} members when the assignment is computed, joining members included
 *     and leaving members excluded, and the joining or leaving members are taken at regular
 *     intervals over the member indices.</li>
 * </ul>
 *
 * <p>The full grid is a menu rather than a run. Three runs cover the points of interest: the
 * first one for scaling, the other two for the cost of the events on a large group with many
 * topics and on a small group with very many topics. The largest points hold ten thousand
 * members subscribing to ten thousand topics, which takes about 4 GB of heap for the
 * subscriptions alone, as it would in the coordinator: pass {@code -jvmArgs -Xmx8g} when the
 * default heap is smaller.
 * <pre>
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p event=FULL,STABLE,JOIN_ONE \
 *     -p topology=EQUAL -p subscription=HOMOGENEOUS ConsumerAssignorBenchmark
 *
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p memberCount=10000 -p topicCount=1000 \
 *     -p subscription=HOMOGENEOUS,NESTED ConsumerAssignorBenchmark
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p memberCount=20 -p topicCount=10000 \
 *     -p subscription=HOMOGENEOUS ConsumerAssignorBenchmark
 * </pre>
 * The GC profiler reports the bytes allocated per assignment, which matter as much as the
 * time since assignments are computed on the coordinator threads.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ConsumerAssignorBenchmark {

    public enum AssignorType {
        RANGE, UNIFORM;

        PartitionAssignor create() {
            return switch (this) {
                case RANGE -> new RangeAssignor();
                case UNIFORM -> new UniformAssignor();
            };
        }
    }

    /** How the partitions are split over the topics. */
    public enum Topology {
        /** Every topic has the same number of partitions, up to the rounding remainder. */
        EQUAL,
        /**
         * The topics form geometric tiers: the first tier holds two thirds of the topics, and
         * every following tier holds a third of the topics of the previous one, with twice as
         * many partitions per topic. This gives a few large topics, a band of small ones and a
         * majority of topics with the smallest size, which is how topics are commonly sized.
         * The tiers need about two partitions per topic; with fewer, the split is equal.
         */
        SKEWED;

        /**
         * @param topicCount        The number of topics.
         * @param partitionCount    The total number of partitions.
         * @return The number of partitions of each topic, largest first and at least one, so
         *         that the total may exceed the requested one when there are more topics than
         *         partitions.
         */
        int[] partitionCounts(int topicCount, int partitionCount) {
            int[] counts = new int[topicCount];
            if (this == SKEWED) {
                List<Integer> tierSizes = new ArrayList<>();
                double share = 2.0 / 3.0;
                for (int remaining = topicCount; remaining > 0; share /= 3.0) {
                    int size = Math.min(remaining, Math.max(1, (int) Math.round(topicCount * share)));
                    tierSizes.add(size);
                    remaining -= size;
                }
                long weight = 0;
                for (int tier = 0; tier < tierSizes.size(); tier++) {
                    weight += (long) tierSizes.get(tier) << tier;
                }
                if (weight <= partitionCount) {
                    long base = partitionCount / weight;
                    int topic = 0;
                    for (int tier = tierSizes.size() - 1; tier >= 0; tier--) {
                        Arrays.fill(counts, topic, topic + tierSizes.get(tier), (int) (base << tier));
                        topic += tierSizes.get(tier);
                    }
                    spreadRemainder(counts, partitionCount);
                    return counts;
                }
            }
            Arrays.fill(counts, Math.max(1, partitionCount / topicCount));
            spreadRemainder(counts, partitionCount);
            return counts;
        }

        /** Hands the partitions not given yet, if any, to the topics one at a time from the first. */
        private static void spreadRemainder(int[] counts, int partitionCount) {
            long remainder = partitionCount - Arrays.stream(counts).asLongStream().sum();
            for (long i = 0; i < remainder; i++) {
                counts[(int) (i % counts.length)]++;
            }
        }
    }

    /** How the members subscribe to the topics. */
    public enum Subscription {
        HOMOGENEOUS, DISJOINT, NESTED;

        SubscriptionType type() {
            return this == HOMOGENEOUS ? SubscriptionType.HOMOGENEOUS : SubscriptionType.HETEROGENEOUS;
        }

        /**
         * @param bucket        The bucket of the members.
         * @param bucketCount   The number of buckets.
         * @param topicNames    All the topics, largest first.
         * @return The topics of the members of the bucket.
         */
        List<String> topics(int bucket, int bucketCount, List<String> topicNames) {
            int topicCount = topicNames.size();
            return switch (this) {
                case HOMOGENEOUS -> topicNames;
                case DISJOINT -> topicNames.subList(topicCount * bucket / bucketCount, topicCount * (bucket + 1) / bucketCount);
                case NESTED -> topicNames.subList(0, topicCount * (bucket + 1) / bucketCount);
            };
        }
    }

    /** Whether the members have a rack. */
    public enum Rack {
        NONE, PROVIDED;

        Optional<String> of(int memberIndex) {
            return this == PROVIDED ? Optional.of(rackId(memberIndex)) : Optional.empty();
        }
    }

    /** What happened to the group before the assignment is computed. */
    public enum Event {
        FULL, STABLE, JOIN_ONE, JOIN_MANY, LEAVE_ONE, LEAVE_MANY, PARTITIONS_ADDED;

        boolean isJoin() {
            return this == JOIN_ONE || this == JOIN_MANY;
        }

        boolean isLeave() {
            return this == LEAVE_ONE || this == LEAVE_MANY;
        }
    }

    /** A member of the group. */
    private record Member(String id, Optional<String> rackId, Set<String> topics) { }

    /**
     * The metadata of the cluster, and the views the assignors take on it. The cluster has
     * {@link #BROKER_COUNT} brokers spread over {@link #RACK_COUNT} racks, and every partition
     * has two replicas on adjacent brokers, so that it is in two of the three racks. Topic ids
     * are drawn from a generator with a fixed seed, so that rebuilding the cluster with more
     * partitions keeps the ids, and the ids are spread like real ones.
     */
    private record Cluster(TopicIds.CachedTopicResolver topicResolver, SubscribedTopicDescriber describer) {
        static Cluster create(List<String> topicNames, int[] partitionCounts) {
            MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
            for (int brokerId = 0; brokerId < BROKER_COUNT; brokerId++) {
                delta.replay(new RegisterBrokerRecord().setBrokerId(brokerId).setRack(rackId(brokerId)));
            }
            Random random = new Random(TOPIC_ID_SEED);
            for (int topic = 0; topic < topicNames.size(); topic++) {
                Uuid topicId = topicId(random);
                delta.replay(new TopicRecord().setTopicId(topicId).setName(topicNames.get(topic)));
                for (int partition = 0; partition < partitionCounts[topic]; partition++) {
                    delta.replay(new PartitionRecord()
                        .setTopicId(topicId)
                        .setPartitionId(partition)
                        .setReplicas(List.of(partition % BROKER_COUNT, (partition + 1) % BROKER_COUNT)));
                }
            }
            CoordinatorMetadataImage image = new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
            return new Cluster(new TopicIds.CachedTopicResolver(image), new SubscribedTopicDescriberImpl(image));
        }

        /** Draws a topic id from the generator, with the same constraints as {@link Uuid#randomUuid()}. */
        private static Uuid topicId(Random random) {
            Uuid uuid = new Uuid(random.nextLong(), random.nextLong());
            while (Uuid.RESERVED.contains(uuid) || uuid.toString().contains("-")) {
                uuid = new Uuid(random.nextLong(), random.nextLong());
            }
            return uuid;
        }
    }

    /** The brokers are spread over this many racks, and so are the members having a rack. */
    private static final int RACK_COUNT = 3;

    /** Two brokers per rack. */
    private static final int BROKER_COUNT = 2 * RACK_COUNT;

    /** The number of member buckets for heterogeneous subscriptions. */
    private static final int BUCKET_COUNT = 5;

    private static final long TOPIC_ID_SEED = 42L;

    @Param({"2", "20", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"10", "1000", "10000"})
    private int topicCount;

    @Param({"10000", "100000"})
    private int partitionCount;

    @Param({"EQUAL", "SKEWED"})
    private Topology topology;

    @Param({"HOMOGENEOUS", "DISJOINT", "NESTED"})
    private Subscription subscription;

    @Param({"NONE", "PROVIDED"})
    private Rack rack;

    @Param({"RANGE", "UNIFORM"})
    private AssignorType assignor;

    @Param({"FULL", "STABLE", "JOIN_ONE", "JOIN_MANY", "LEAVE_ONE", "LEAVE_MANY", "PARTITIONS_ADDED"})
    private Event event;

    private PartitionAssignor partitionAssignor;

    private TopicIds.CachedTopicResolver topicResolver;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    private GroupSpec groupSpec;

    @Setup(Level.Trial)
    public void setup() {
        partitionAssignor = assignor.create();
        List<String> topicNames = AssignorBenchmarkUtils.createTopicNames(topicCount);
        int[] partitionCounts = topology.partitionCounts(topicCount, partitionCount);
        Cluster cluster = Cluster.create(topicNames, partitionCounts);

        // The joining members are not in the group yet when the previous assignment is computed,
        // and the leaving members are not in the group anymore when the assignment is measured.
        int changedCount = changedMemberCount();
        List<Member> allMembers = createMembers(event.isLeave() ? memberCount + changedCount : memberCount, topicNames);
        Set<String> changedMemberIds = spreadMemberIds(allMembers.size(), changedCount);
        List<Member> previousMembers = event.isJoin() ? without(allMembers, changedMemberIds) : allMembers;
        List<Member> members = event.isLeave() ? without(allMembers, changedMemberIds) : allMembers;

        GroupAssignment previousAssignment = new GroupAssignment(Map.of());
        if (event != Event.FULL) {
            previousAssignment = partitionAssignor.assign(
                groupSpec(previousMembers, cluster, previousAssignment),
                cluster.describer()
            );
        }

        if (event == Event.PARTITIONS_ADDED) {
            for (int topic = 0; topic < topicCount; topic += 10) {
                partitionCounts[topic]++;
            }
            cluster = Cluster.create(topicNames, partitionCounts);
        }

        groupSpec = groupSpec(members, cluster, previousAssignment);
        topicResolver = cluster.topicResolver();
        subscribedTopicDescriber = cluster.describer();
    }

    private int changedMemberCount() {
        return switch (event) {
            case JOIN_ONE, LEAVE_ONE -> 1;
            case JOIN_MANY, LEAVE_MANY -> (memberCount + 9) / 10;
            default -> 0;
        };
    }

    /**
     * @return The given number of members, member {@code i} being in bucket {@code i mod
     *         bucketCount}. The members of a bucket share their topic set.
     */
    private List<Member> createMembers(int count, List<String> topicNames) {
        int bucketCount = Math.min(BUCKET_COUNT, Math.min(memberCount, topicCount));
        List<Set<String>> bucketTopics = new ArrayList<>(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            bucketTopics.add(new HashSet<>(subscription.topics(bucket, bucketCount, topicNames)));
        }
        List<Member> members = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            members.add(new Member("member" + i, rack.of(i), bucketTopics.get(i % bucketCount)));
        }
        return members;
    }

    /**
     * @return The ids of {@code count} members taken at regular intervals over the {@code total}
     *         members.
     */
    private static Set<String> spreadMemberIds(int total, int count) {
        Set<String> memberIds = new HashSet<>();
        for (int i = 0; i < count; i++) {
            memberIds.add("member" + (int) ((long) i * total / count));
        }
        return memberIds;
    }

    private static List<Member> without(List<Member> members, Set<String> memberIds) {
        List<Member> remaining = new ArrayList<>(members.size());
        for (Member member : members) {
            if (!memberIds.contains(member.id())) {
                remaining.add(member);
            }
        }
        return remaining;
    }

    /**
     * @return The spec of the group, the members holding the partitions the previous assignment
     *         gave them. The partitions of members no longer in the group are unassigned.
     */
    private GroupSpec groupSpec(List<Member> members, Cluster cluster, GroupAssignment previousAssignment) {
        Map<String, MemberSubscriptionAndAssignmentImpl> memberSpecs = new HashMap<>();
        Map<String, MemberAssignment> currentAssignments = new HashMap<>();
        for (Member member : members) {
            MemberAssignment currentAssignment = previousAssignment.members().get(member.id());
            Map<Uuid, Set<Integer>> partitions = Map.of();
            if (currentAssignment != null) {
                currentAssignments.put(member.id(), currentAssignment);
                partitions = currentAssignment.partitions();
            }
            memberSpecs.put(member.id(), new MemberSubscriptionAndAssignmentImpl(
                member.rackId(),
                Optional.empty(),
                new TopicIds(member.topics(), cluster.topicResolver()),
                new Assignment(partitions)
            ));
        }
        return new GroupSpecImpl(
            memberSpecs,
            subscription.type(),
            AssignorBenchmarkUtils.computeInvertedTargetAssignment(new GroupAssignment(currentAssignments))
        );
    }

    /**
     * @return The rack of the member or broker with the given index.
     */
    private static String rackId(int index) {
        return "rack" + (index % RACK_COUNT);
    }

    @Benchmark
    @Threads(1)
    public GroupAssignment doAssignment() {
        topicResolver.clear();
        return partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
    }
}
