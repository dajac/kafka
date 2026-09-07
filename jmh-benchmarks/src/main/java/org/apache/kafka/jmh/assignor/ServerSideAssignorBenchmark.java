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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.Uniform2Assignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ServerSideAssignorBenchmark {

    public enum AssignorType {
        RANGE(new RangeAssignor()),
        UNIFORM(new UniformAssignor()),
        UNIFORM2(new Uniform2Assignor());

        private final PartitionAssignor assignor;

        AssignorType(PartitionAssignor assignor) {
            this.assignor = assignor;
        }

        public PartitionAssignor assignor() {
            return assignor;
        }
    }

    /**
     * The assignment type is decided based on whether all the members are assigned partitions
     * for the first time (full), or incrementally when a rebalance is triggered.
     */
    public enum AssignmentType {
        FULL, INCREMENTAL
    }

    @Param({"100", "500", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"5", "10", "50"})
    private int partitionsToMemberRatio;

    @Param({"10", "100", "1000"})
    private int topicCount;

    @Param({"true", "false"})
    private boolean isRackAware;

    @Param({"HOMOGENEOUS", "HETEROGENEOUS"})
    private SubscriptionType subscriptionType;

    @Param({"RANGE", "UNIFORM", "UNIFORM2"})
    private AssignorType assignorType;

    @Param({"FULL", "INCREMENTAL"})
    private AssignmentType assignmentType;

    public enum TopicSizeDistribution {
        EQUAL, MIXED
    }

    @Param({"EQUAL"})
    private TopicSizeDistribution topicSizeDistribution;

    private PartitionAssignor partitionAssignor;

    private static final int NUMBER_OF_RACKS = 3;

    /** The number of homogeneous subgroups to create for the heterogeneous subscription case. */
    private static final int MAX_BUCKET_COUNT = 5;

    private GroupSpec groupSpec;

    private List<String> allTopicNames = List.of();

    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    private TopicIds.TopicResolver topicResolver;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    @Setup(Level.Trial)
    public void setup() {
        partitionAssignor = assignorType.assignor();
        if (assignorType == AssignorType.UNIFORM2) {
            Uniform2Assignor uniform2 = new Uniform2Assignor();
            uniform2.configure(Map.of(Uniform2Assignor.RACK_AWARE_CONFIG, isRackAware));
            partitionAssignor = uniform2;
        }

        setupTopics();

        Map<String, ConsumerGroupMember> members = createMembers();
        this.groupSpec = AssignorBenchmarkUtils.createConsumerGroupSpec(members, subscriptionType, topicResolver);

        if (assignmentType == AssignmentType.INCREMENTAL) {
            simulateIncrementalRebalance();
        }
    }

    private void setupTopics() {
        allTopicNames = AssignorBenchmarkUtils.createTopicNames(topicCount);

        int partitionsPerTopic = (memberCount * partitionsToMemberRatio) / topicCount;

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        int[] counts = new int[topicCount];
        Arrays.fill(counts, partitionsPerTopic);
        if (topicSizeDistribution == TopicSizeDistribution.MIXED) {
            // Keep the same partition budget, with 80% singletons and differently sized
            // larger topics. Interleave them so each heterogeneous bucket sees both.
            if (topicCount < 5 || memberCount * partitionsToMemberRatio < topicCount) {
                throw new IllegalArgumentException("MIXED requires at least five topics and one partition per topic");
            }
            Arrays.fill(counts, 1);
            int remaining = memberCount * partitionsToMemberRatio - topicCount;
            int weightSum = 0;
            for (int i = 4; i < topicCount; i += 5) weightSum += 1 + (i / 5) % 4;
            int allocated = 0;
            for (int i = 4; i < topicCount; i += 5) {
                int extra = (int) ((long) remaining * (1 + (i / 5) % 4) / weightSum);
                counts[i] += extra;
                allocated += extra;
            }
            counts[topicCount - 1 - (topicCount % 5)] += remaining - allocated;
        }
        for (int i = 0; i < topicCount; i++) {
            // Stable IDs make topic traversal identical in paired benchmark runs.
            AssignorBenchmarkUtils.addTopic(delta, new Uuid(0, i + 1), allTopicNames.get(i), counts[i]);
        }
        metadataImage = new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
        topicResolver = new TopicIds.CachedTopicResolver(metadataImage);

        SubscribedTopicDescriber metadata = new SubscribedTopicDescriberImpl(metadataImage);
        // RF=2 across three racks exercises partial locality. The metadata helper above
        // does not register broker racks, so member rack IDs alone would not test locality.
        List<Set<String>> replicaRacks = List.of(
            Set.of("rack0", "rack1"), Set.of("rack1", "rack2"), Set.of("rack2", "rack0"));
        subscribedTopicDescriber = new SubscribedTopicDescriber() {
            @Override
            public int numPartitions(Uuid topicId) {
                return metadata.numPartitions(topicId);
            }

            @Override
            public Set<String> racksForPartition(Uuid topicId, int partition) {
                return isRackAware ? replicaRacks.get(partition % NUMBER_OF_RACKS) : Set.of();
            }
        };
    }

    private Map<String, ConsumerGroupMember> createMembers() {
        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = assignmentType.equals(AssignmentType.INCREMENTAL) ? memberCount - 1 : memberCount;

        if (subscriptionType == HOMOGENEOUS) {
            return AssignorBenchmarkUtils.createHomogeneousMembers(
                numberOfMembers,
                this::memberId,
                this::rackId,
                allTopicNames
            );
        } else {
            return AssignorBenchmarkUtils.createHeterogeneousBucketedMembers(
                numberOfMembers,
                MAX_BUCKET_COUNT,
                this::memberId,
                this::rackId,
                allTopicNames
            );
        }
    }

    private String memberId(int memberIndex) {
        return "member" + memberIndex;
    }

    private Optional<String> rackId(int memberIndex) {
        return isRackAware ? Optional.of("rack" + memberIndex % NUMBER_OF_RACKS) : Optional.empty();
    }

    private void simulateIncrementalRebalance() {
        GroupAssignment initialAssignment = partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
        Map<String, MemberAssignment> members = initialAssignment.members();

        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = AssignorBenchmarkUtils.computeInvertedTargetAssignment(initialAssignment);

        Map<String, MemberSubscriptionAndAssignmentImpl> updatedMemberSpec = new HashMap<>();

        for (String memberId : groupSpec.memberIds()) {
            MemberAssignment memberAssignment = members.getOrDefault(
                memberId,
                new MemberAssignmentImpl(Map.of())
            );

            updatedMemberSpec.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                groupSpec.memberSubscription(memberId).rackId(),
                Optional.empty(),
                groupSpec.memberSubscription(memberId).subscribedTopicIds(),
                new Assignment(Map.copyOf(memberAssignment.partitions()))
            ));
        }

        Set<Uuid> subscribedTopicIdsForNewMember;
        if (subscriptionType == HETEROGENEOUS) {
            subscribedTopicIdsForNewMember = updatedMemberSpec.get(memberId(memberCount - 2)).subscribedTopicIds();
        } else {
            subscribedTopicIdsForNewMember = new TopicIds(new HashSet<>(allTopicNames), topicResolver);
        }

        Optional<String> rackId = rackId(memberCount - 1);
        updatedMemberSpec.put("newMember", new MemberSubscriptionAndAssignmentImpl(
            rackId,
            Optional.empty(),
            subscribedTopicIdsForNewMember,
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            updatedMemberSpec,
            subscriptionType,
            invertedTargetAssignment
        );
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public GroupAssignment doAssignment() {
        topicResolver.clear();
        return partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
    }
}
