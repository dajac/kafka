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
 *     <li>{@code topology}: how the partitions are split over the topics, see {@link Topology}.
 *     Every topic has at least one partition.</li>
 *     <li>{@code subscription}: how the members subscribe, see {@link Subscription}. The
 *     heterogeneous subscriptions put the members in five buckets, member {@code i} being in
 *     bucket {@code i mod 5}, see {@link GroupBuilder}.</li>
 *     <li>{@code rack}: whether the members have a rack, see {@link Rack}. Assignors which do
 *     not use racks give the same results for both values.</li>
 *     <li>{@code assignor}: the assignor.</li>
 *     <li>{@code event}: the state of the group, see {@link Event}. The group always has
 *     {@code memberCount} members when the assignment is computed, joining members included
 *     and leaving members excluded. The joining or leaving members are the ones with the highest
 *     indices, so they are spread over the buckets.</li>
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
 *     -p subscription=HOMOGENEOUS,HETEROGENEOUS_NESTED ConsumerAssignorBenchmark
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

    /**
     * The assignor computing the assignment.
     */
    public enum AssignorType {
        /**
         * The range assignor.
         */
        RANGE,

        /**
         * The uniform assignor.
         */
        UNIFORM
    }

    /**
     * How the partitions are split over the topics.
     */
    public enum Topology {
        /**
         * Every topic has the same number of partitions, up to the rounding remainder.
         */
        EQUAL,

        /**
         * The topics form geometric tiers: the first tier holds two thirds of the topics, and
         * every following tier holds a third of the topics of the previous one, with twice as
         * many partitions per topic. This gives a few large topics, a band of small ones and a
         * majority of topics with the smallest size, which is how topics are commonly sized.
         * The tiers need about two partitions per topic; with fewer, the split is equal.
         */
        SKEWED
    }

    /**
     * How the members subscribe to the topics.
     */
    public enum Subscription {
        /**
         * Every member subscribes to every topic.
         */
        HOMOGENEOUS,

        /**
         * The members of a bucket subscribe to their own share of the topics, so that no topic
         * is shared by two buckets.
         */
        HETEROGENEOUS_DISJOINT,

        /**
         * The members of bucket {@code b} subscribe to the first {@code b + 1} shares of the
         * topics, so that the first share is subscribed by every member and the last one by the
         * members of the last bucket only.
         */
        HETEROGENEOUS_NESTED
    }

    /**
     * Whether the members have a rack.
     */
    public enum Rack {
        /**
         * The members have no rack.
         */
        NONE,

        /**
         * The members are spread over the racks of the brokers, member {@code i} being in rack
         * {@code i mod 3}.
         */
        PROVIDED
    }

    /**
     * What happened to the group before the assignment is computed.
     */
    public enum Event {
        /**
         * No member holds partitions.
         */
        FULL,

        /**
         * The members hold the output of the assignor for the same group, so nothing has to
         * change.
         */
        STABLE,

        /**
         * One member joined and holds nothing.
         */
        JOIN_ONE,

        /**
         * A tenth of the members, rounded up, joined and hold nothing.
         */
        JOIN_MANY,

        /**
         * One member left, leaving its partitions unassigned.
         */
        LEAVE_ONE,

        /**
         * A tenth of the members, rounded up, left, leaving their partitions unassigned.
         */
        LEAVE_MANY,

        /**
         * One topic in ten gained a partition.
         */
        PARTITIONS_ADDED
    }

    /**
     * The input of an assignment: the spec of the group, and the views of the cluster metadata
     * the assignor takes. The resolver is cleared before every assignment, as the coordinator
     * uses a new one for every assignment.
     */
    private record Group(
        GroupSpec spec,
        TopicIds.CachedTopicResolver topicResolver,
        SubscribedTopicDescriber describer
    ) { }

    /**
     * Builds the input of an assignment. Every build creates the metadata image of the cluster
     * and new views of it, so that nothing is shared between the groups built.
     *
     * <p>The topics are named by {@link AssignorBenchmarkUtils#createTopicNames}, and the
     * partitions are split over them as the topology says, see {@link #partitionCounts}, the
     * largest topics first. When partitions were added, one topic in
     * {@link #ADDED_PARTITIONS_TOPIC_STRIDE} has one more partition than the split gives it.
     *
     * <p>The cluster has {@link #BROKER_COUNT} brokers spread over {@link #RACK_COUNT} racks,
     * and every partition has two replicas on adjacent brokers, so that it is in two of the
     * three racks. Topic ids are drawn from a generator with a fixed seed, so that building the
     * cluster again with more partitions keeps the ids, and the ids are spread like real ones.
     *
     * <p>Member {@code i} is called {@code member<i>}, and is in bucket {@code i mod bucketCount}
     * for the heterogeneous subscriptions, so that members added at the end are spread over the
     * buckets. The bucket count is fixed by the caller rather than derived from the member
     * count, so that the topics of a bucket are the same in groups built with different member
     * counts. Bucket {@code b} owns the {@code b}-th share of the topics, the shares being
     * consecutive ranges of about the same size. With two members and two buckets, a joining
     * member brings a bucket nobody subscribed to before; from ten members on, every bucket
     * keeps members through the events.
     */
    private static final class GroupBuilder {
        private int topicCount = 0;
        private int partitionCount = 0;
        private Topology topology = Topology.EQUAL;
        private boolean partitionsAdded = false;
        private Subscription subscription = Subscription.HOMOGENEOUS;
        private Rack rack = Rack.NONE;
        private int bucketCount = 1;
        private int memberCount = 0;
        private GroupAssignment currentAssignment = new GroupAssignment(Map.of());

        GroupBuilder withTopicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        /**
         * @param partitionCount    The total number of partitions over all topics.
         */
        GroupBuilder withPartitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        GroupBuilder withTopology(Topology topology) {
            this.topology = topology;
            return this;
        }

        /**
         * @param partitionsAdded   Whether one topic in {@link #ADDED_PARTITIONS_TOPIC_STRIDE}
         *                          gained a partition.
         */
        GroupBuilder withPartitionsAdded(boolean partitionsAdded) {
            this.partitionsAdded = partitionsAdded;
            return this;
        }

        GroupBuilder withSubscription(Subscription subscription) {
            this.subscription = subscription;
            return this;
        }

        GroupBuilder withRack(Rack rack) {
            this.rack = rack;
            return this;
        }

        GroupBuilder withBucketCount(int bucketCount) {
            this.bucketCount = bucketCount;
            return this;
        }

        GroupBuilder withMemberCount(int memberCount) {
            this.memberCount = memberCount;
            return this;
        }

        /**
         * @param currentAssignment The partitions the members hold. Members without an entry
         *                          hold nothing, and the entries of members not in the group
         *                          are ignored.
         */
        GroupBuilder withCurrentAssignment(GroupAssignment currentAssignment) {
            this.currentAssignment = currentAssignment;
            return this;
        }

        Group build() {
            List<String> topicNames = AssignorBenchmarkUtils.createTopicNames(topicCount);
            int[] partitionCounts = partitionCounts(topology, topicCount, partitionCount);
            if (partitionsAdded) {
                for (int topic = 0; topic < topicCount; topic += ADDED_PARTITIONS_TOPIC_STRIDE) {
                    partitionCounts[topic]++;
                }
            }
            CoordinatorMetadataImage image = createImage(topicNames, partitionCounts);
            TopicIds.CachedTopicResolver topicResolver = new TopicIds.CachedTopicResolver(image);

            List<Set<String>> bucketTopics = new ArrayList<>(bucketCount);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                bucketTopics.add(new HashSet<>(topicsOfBucket(bucket, topicNames)));
            }

            Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
            Map<String, MemberAssignment> memberAssignments = new HashMap<>();
            for (int i = 0; i < memberCount; i++) {
                String memberId = "member" + i;
                MemberAssignment memberAssignment = currentAssignment.members().get(memberId);
                Map<Uuid, Set<Integer>> partitions = Map.of();
                if (memberAssignment != null) {
                    memberAssignments.put(memberId, memberAssignment);
                    partitions = memberAssignment.partitions();
                }
                members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                    rack == Rack.NONE ? Optional.empty() : Optional.of(rackId(i)),
                    Optional.empty(),
                    new TopicIds(bucketTopics.get(i % bucketCount), topicResolver),
                    new Assignment(partitions)
                ));
            }

            GroupSpec spec = new GroupSpecImpl(
                members,
                subscription == Subscription.HOMOGENEOUS ? SubscriptionType.HOMOGENEOUS : SubscriptionType.HETEROGENEOUS,
                AssignorBenchmarkUtils.computeInvertedTargetAssignment(new GroupAssignment(memberAssignments))
            );
            return new Group(spec, topicResolver, new SubscribedTopicDescriberImpl(image));
        }

        private static CoordinatorMetadataImage createImage(List<String> topicNames, int[] partitionCounts) {
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
            return new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
        }

        /**
         * Draws a topic id from the generator, with the same constraints as
         * {@link Uuid#randomUuid()}.
         */
        private static Uuid topicId(Random random) {
            Uuid uuid = new Uuid(random.nextLong(), random.nextLong());
            while (Uuid.RESERVED.contains(uuid) || uuid.toString().contains("-")) {
                uuid = new Uuid(random.nextLong(), random.nextLong());
            }
            return uuid;
        }

        /**
         * @return The topics the members of the bucket subscribe to.
         */
        private List<String> topicsOfBucket(int bucket, List<String> topicNames) {
            return switch (subscription) {
                case HOMOGENEOUS -> topicNames;
                case HETEROGENEOUS_DISJOINT -> topicNames.subList(
                    topicCount * bucket / bucketCount,
                    topicCount * (bucket + 1) / bucketCount
                );
                case HETEROGENEOUS_NESTED -> topicNames.subList(0, topicCount * (bucket + 1) / bucketCount);
            };
        }

        /**
         * @return The rack of the member or broker with the given index.
         */
        private static String rackId(int index) {
            return "rack" + (index % RACK_COUNT);
        }

    /**
         * @param topology          How the partitions are split over the topics.
         * @param topicCount        The number of topics.
         * @param partitionCount    The total number of partitions.
         * @return The number of partitions of each topic, largest first and at least one, so that
         *         the total may exceed the requested one when there are more topics than partitions.
         */
        private static int[] partitionCounts(Topology topology, int topicCount, int partitionCount) {
            int[] counts = new int[topicCount];
            if (topology == Topology.SKEWED) {
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

        /**
         * Hands the partitions not given yet, if any, to the topics one at a time from the first.
         */
        private static void spreadRemainder(int[] counts, int partitionCount) {
            long remainder = partitionCount - Arrays.stream(counts).asLongStream().sum();
            for (long i = 0; i < remainder; i++) {
                counts[(int) (i % counts.length)]++;
            }
        }
    }

    /**
     * The brokers are spread over this many racks, and so are the members having a rack.
     */
    private static final int RACK_COUNT = 3;

    /**
     * Two brokers per rack.
     */
    private static final int BROKER_COUNT = 2 * RACK_COUNT;

    /**
     * The number of member buckets for the heterogeneous subscriptions, when the group has that
     * many members and topics.
     */
    private static final int BUCKET_COUNT = 5;

    /**
     * The events on many members change one member in this many.
     */
    private static final int MANY_MEMBERS_DIVISOR = 10;

    /**
     * The partitions added event adds a partition to one topic in this many.
     */
    private static final int ADDED_PARTITIONS_TOPIC_STRIDE = 10;

    private static final long TOPIC_ID_SEED = 42L;

    @Param({"2", "20", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"10", "1000", "10000"})
    private int topicCount;

    @Param({"10000", "100000"})
    private int partitionCount;

    @Param({"EQUAL", "SKEWED"})
    private Topology topology;

    @Param({"HOMOGENEOUS", "HETEROGENEOUS_DISJOINT", "HETEROGENEOUS_NESTED"})
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
        partitionAssignor = createAssignor();

        GroupBuilder builder = new GroupBuilder()
            .withTopicCount(topicCount)
            .withPartitionCount(partitionCount)
            .withTopology(topology)
            .withSubscription(subscription)
            .withRack(rack)
            .withBucketCount(Math.min(BUCKET_COUNT, Math.min(memberCount, topicCount)));

        // The previous assignment is the output of the assignor for the group as it was before
        // the event: without the joining members, with the leaving members, and before the
        // partitions were added.
        GroupAssignment previousAssignment = new GroupAssignment(Map.of());
        if (event != Event.FULL) {
            Group previousGroup = builder.withMemberCount(previousMemberCount()).build();
            previousAssignment = partitionAssignor.assign(previousGroup.spec(), previousGroup.describer());
        }

        Group group = builder
            .withMemberCount(memberCount)
            .withPartitionsAdded(event == Event.PARTITIONS_ADDED)
            .withCurrentAssignment(previousAssignment)
            .build();
        groupSpec = group.spec();
        topicResolver = group.topicResolver();
        subscribedTopicDescriber = group.describer();
    }

    private PartitionAssignor createAssignor() {
        return switch (assignor) {
            case RANGE -> new RangeAssignor();
            case UNIFORM -> new UniformAssignor();
        };
    }

    /**
     * @return The number of members of the group before the event.
     */
    private int previousMemberCount() {
        // A tenth of the members, rounded up.
        int manyMembers = (memberCount + MANY_MEMBERS_DIVISOR - 1) / MANY_MEMBERS_DIVISOR;
        return switch (event) {
            case JOIN_ONE -> memberCount - 1;
            case JOIN_MANY -> memberCount - manyMembers;
            case LEAVE_ONE -> memberCount + 1;
            case LEAVE_MANY -> memberCount + manyMembers;
            default -> memberCount;
        };
    }

    @Benchmark
    @Threads(1)
    public GroupAssignment doAssignment() {
        topicResolver.clear();
        return partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
    }
}
