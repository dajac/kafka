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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The input of the uniform2 assignor, normalized once per assignment and read by every phase.
 *
 * <p>Members and topics are sorted by id and numbered from zero. Everything below refers to them
 * by index, and every array indexed by member or topic follows that numbering. Lists attached to
 * a member or a topic are stored as compressed rows: the entries of item {@code i} are the values
 * from {@code start[i]} inclusive to {@code start[i + 1]} exclusive. The model does not change
 * once built.
 */
final class GroupModel {
    /** Marks the absence of a member, topic or partition index. */
    static final int NONE = -1;

    /** Rack awareness handles at most this many racks, one bit per rack in a long. */
    static final int MAX_RACKS = 64;

    /**
     * The largest number of members times topics for which the relations between members and
     * topics that the phases look up constantly are kept as bitsets, see {@link #isBacked} and
     * {@link ExtraPartitions#has}: 4 million bits, 512 KB per bitset. Below it, a bitset
     * is cheap to allocate and clear for every assignment and answers in constant time, where a
     * binary search in the sorted topics of a member costs a dozen comparisons in a group with
     * many topics. Above it, the bitsets would weigh on every assignment while buying little:
     * such a group has many members, so each of them has few extra partitions and a short
     * search, and with 10,000 members and 10,000 topics each bitset would take 12.5 MB, where
     * the sorted arrays only hold the pairs actually related.
     */
    static final long MAX_BITSET_BITS = 1L << 22;

    /** The number of members. */
    final int memberCount;
    /** The member ids, sorted. */
    final String[] memberIds;
    /** Per member, its current assignment as given in the input. */
    final Map<Uuid, Set<Integer>>[] currentAssignments;
    /**
     * Per member, whether some entries of its current assignment are dropped because they refer
     * to topics that no longer exist or are no longer subscribed, or hold no partition.
     */
    final boolean[] hasDroppedPartitions;

    /** The number of subscribed topics. */
    final int topicCount;
    /** The topic ids, sorted. */
    final Uuid[] topicIds;
    /** Maps topic ids to their index. */
    final TopicIndex topicIndex;
    /** Per topic, its number of partitions. */
    final int[] partitionCounts;
    /** Per topic, the base partitions: the number every subscriber gets. */
    final int[] basePartitionCount;
    /** Per topic, the number of extra partitions, each going to a distinct subscriber. */
    final int[] extraPartitionCount;
    /** Whether the group is small enough for bitsets over its topics and members, see {@link #MAX_BITSET_BITS}. */
    final boolean usesBitsets;

    /** Whether all members have the same subscription. */
    final boolean homogeneous;
    /** Per topic, its subscribers in ascending order. Every topic shares one array when homogeneous. */
    final int[][] subscribers;
    /** Per member, its topics in ascending order. Every member shares one array when homogeneous. */
    final int[][] memberTopics;

    /** Per topic, the compressed row of its current holders in {@link #holderMember}. */
    final int[] holderStart;
    /** The current holders of every topic, in ascending member order within a topic. */
    final int[] holderMember;
    /** The current partitions of every holder, parallel to {@link #holderMember}. */
    final Set<Integer>[] holderPartitions;
    /**
     * Per holder, how many of its current partitions exist, that is have an id below the
     * partition count of the topic. The others are dropped.
     */
    final int[] holderValidCount;
    /** Per member, the compressed row of its backed topics in {@link #backedTopics}. */
    final int[] backedStart;
    /** Per member, the topics of which it currently holds more than the base partitions, ascending. */
    final int[] backedTopics;
    /**
     * Per member and topic, whether the member currently holds more than the base partitions of
     * the topic, indexed by {@link #bitIndex}, or null when the group is too large for a bitset.
     */
    private final long[] backedBits;

    /** The number of cohorts: groups of members with the same subscription, and rack when in use. */
    final int cohortCount;
    /** Per member, its cohort. */
    final int[] memberCohort;
    /** Per cohort, its rack, or zero when racks are not in use. */
    final int[] cohortRack;
    /** Per cohort, the base load of its members: the sum of the base partitions of their topics. */
    final int[] cohortBaseLoad;
    /** Per cohort, its number of members. */
    final int[] cohortSize;
    /** Per cohort, its topics in ascending order. */
    final int[][] cohortTopics;
    /** Per topic, the compressed row of its cohorts in {@link #topicCohorts}. */
    final int[] topicCohortStart;
    /** The cohorts subscribed to every topic. */
    final int[] topicCohorts;

    /** Whether rack awareness is in use: enabled, every member has a rack and 2 to 64 racks. */
    final boolean usesRacks;
    /** The number of member racks, when in use. */
    final int rackCount;
    /** Per member, its rack, when in use. */
    final int[] memberRack;
    /** Per topic and partition, the racks having a replica, one bit per rack, when in use. */
    final long[][] partitionRacks;
    /** Per topic and rack, the number of partitions with a replica in the rack, when in use. */
    final int[][] rackSupply;
    /** Per topic and rack, the number of subscribers of the topic in the rack, when in use. */
    final int[][] rackSubscribers;

    GroupModel(
        GroupSpec groupSpec,
        SubscribedTopicDescriber describer,
        boolean rackAwareEnabled
    ) {
        this(groupSpec, describer, rackAwareEnabled, MAX_BITSET_BITS);
    }

    /**
     * @param maxBitsetBits The largest number of members times topics for which bitsets are used,
     *                      {@link #MAX_BITSET_BITS} in production. Tests pass other values to
     *                      exercise both representations on the same group.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    GroupModel(
        GroupSpec groupSpec,
        SubscribedTopicDescriber describer,
        boolean rackAwareEnabled,
        long maxBitsetBits
    ) {
        homogeneous = groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS;

        memberIds = groupSpec.memberIds().toArray(new String[0]);
        Arrays.sort(memberIds);
        memberCount = memberIds.length;

        topicIds = subscribedTopicIds(groupSpec);
        topicCount = topicIds.length;
        topicIndex = new TopicIndex(topicIds);
        partitionCounts = partitionCounts(describer);
        usesBitsets = (long) memberCount * topicCount <= maxBitsetBits;

        Subscriptions subscriptions = homogeneous ? homogeneousSubscriptions() : heterogeneousSubscriptions(groupSpec);
        subscribers = subscriptions.subscribers;
        memberTopics = subscriptions.memberTopics;
        basePartitionCount = new int[topicCount];
        extraPartitionCount = new int[topicCount];
        for (int t = 0; t < topicCount; t++) {
            int subscriberCount = subscribers[t].length;
            basePartitionCount[t] = partitionCounts[t] / subscriberCount;
            extraPartitionCount[t] = partitionCounts[t] % subscriberCount;
        }

        Racks racks = racks(groupSpec, describer, rackAwareEnabled);
        usesRacks = racks.count > 0;
        rackCount = racks.count;
        memberRack = racks.memberRack;
        partitionRacks = racks.partitionRacks;
        rackSupply = racks.supply;

        Cohorts cohorts = homogeneous ? homogeneousCohorts() : heterogeneousCohorts();
        cohortCount = cohorts.count;
        memberCohort = cohorts.memberCohort;
        cohortRack = cohorts.rack;
        cohortTopics = cohorts.topics;
        CohortIndex cohortIndex = indexCohorts();
        cohortBaseLoad = cohortIndex.baseLoad;
        cohortSize = cohortIndex.size;
        topicCohortStart = cohortIndex.topicCohortStart;
        topicCohorts = cohortIndex.topicCohorts;
        rackSubscribers = cohortIndex.rackSubscribers;

        currentAssignments = (Map<Uuid, Set<Integer>>[]) new Map[memberCount];
        hasDroppedPartitions = new boolean[memberCount];
        Holders holders = holders(groupSpec);
        holderStart = holders.start;
        holderMember = holders.member;
        holderPartitions = holders.partitions;
        holderValidCount = holders.validCount;
        Backed backed = backed();
        backedStart = backed.start;
        backedTopics = backed.topics;
        backedBits = backed.bits;
    }

    /**
     * @return Whether the member is subscribed to the topic.
     */
    boolean isSubscribed(int member, int topic) {
        return homogeneous || Arrays.binarySearch(memberTopics[member], topic) >= 0;
    }

    /**
     * @return Whether the member currently holds more partitions of the topic than the base
     *         partitions, so that an extra partition of the topic lets it keep one of them.
     */
    boolean isBacked(int member, int topic) {
        if (backedBits != null) {
            int bit = bitIndex(member, topic);
            return (backedBits[bit >>> 6] & (1L << bit)) != 0;
        }
        return Arrays.binarySearch(backedTopics, backedStart[member], backedStart[member + 1], topic) >= 0;
    }

    /**
     * @return The number of topics of which the member currently holds more than the base partitions.
     */
    int backedCount(int member) {
        return backedStart[member + 1] - backedStart[member];
    }

    /**
     * @return A cleared bitset with one bit per topic and member, indexed by {@link #bitIndex}.
     *         Only for groups small enough for one, see {@link #MAX_BITSET_BITS}.
     */
    long[] newBitset() {
        return new long[(int) (((long) memberCount * topicCount + 63) >>> 6)];
    }

    /**
     * @return The bit of the member and topic in a bitset of {@link #newBitset}. The topic comes
     *         first so that the lookups of the members of one topic, which is how the phases
     *         scan, touch neighbouring bits.
     */
    int bitIndex(int member, int topic) {
        return topic * memberCount + member;
    }

    /**
     * @return The largest number of cohorts subscribed to a single topic.
     */
    int maxCohortsPerTopic() {
        int max = 0;
        for (int t = 0; t < topicCount; t++) {
            max = Math.max(max, topicCohortStart[t + 1] - topicCohortStart[t]);
        }
        return max;
    }

    /**
     * @return The largest number of partitions of a single topic.
     */
    int maxPartitionsPerTopic() {
        int max = 0;
        for (int t = 0; t < topicCount; t++) {
            max = Math.max(max, partitionCounts[t]);
        }
        return max;
    }

    private static int[] range(int count) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = i;
        }
        return values;
    }

    /**
     * @return The subscribed topics, sorted. When homogeneous, the subscription of the first
     *         member is the subscription of every member.
     */
    private Uuid[] subscribedTopicIds(GroupSpec groupSpec) {
        Set<Uuid> topicSet = new HashSet<>();
        if (homogeneous) {
            for (Uuid topicId : groupSpec.memberSubscription(memberIds[0]).subscribedTopicIds()) {
                topicSet.add(topicId);
            }
        } else {
            for (String memberId : memberIds) {
                for (Uuid topicId : groupSpec.memberSubscription(memberId).subscribedTopicIds()) {
                    topicSet.add(topicId);
                }
            }
        }
        Uuid[] sorted = topicSet.toArray(new Uuid[0]);
        Arrays.sort(sorted);
        return sorted;
    }

    private int[] partitionCounts(SubscribedTopicDescriber describer) {
        int[] counts = new int[topicCount];
        for (int t = 0; t < topicCount; t++) {
            int numPartitions = describer.numPartitions(topicIds[t]);
            if (numPartitions == -1) {
                throw new PartitionAssignorException("Members are subscribed to topic " + topicIds[t]
                    + " which doesn't exist in the topic metadata.");
            }
            counts[t] = numPartitions;
        }
        return counts;
    }

    /**
     * @return Per member, the indices of its subscribed topics in ascending order.
     */
    private int[][] memberTopics(GroupSpec groupSpec) {
        int[][] topics = new int[memberCount][];
        IntList buffer = new IntList(16);
        for (int m = 0; m < memberCount; m++) {
            buffer.clear();
            for (Uuid topicId : groupSpec.memberSubscription(memberIds[m]).subscribedTopicIds()) {
                int t = topicIndex.indexOf(topicId);
                if (t != TopicIndex.NONE) {
                    buffer.add(t);
                }
            }
            topics[m] = buffer.toSortedArray();
        }
        return topics;
    }

    /**
     * @return Per topic, the members subscribed to it in ascending order.
     */
    private static int[][] invert(int[][] memberTopics, int topicCount) {
        int[] counts = new int[topicCount];
        for (int[] topics : memberTopics) {
            for (int t : topics) {
                counts[t]++;
            }
        }
        int[][] subscribers = new int[topicCount][];
        for (int t = 0; t < topicCount; t++) {
            subscribers[t] = new int[counts[t]];
            counts[t] = 0;
        }
        for (int m = 0; m < memberTopics.length; m++) {
            for (int t : memberTopics[m]) {
                subscribers[t][counts[t]++] = m;
            }
        }
        return subscribers;
    }

    private record Subscriptions(int[][] subscribers, int[][] memberTopics) { }

    /**
     * With a single subscription, every topic has every member and every member has every topic:
     * one array of all members and one of all topics are shared.
     */
    private Subscriptions homogeneousSubscriptions() {
        int[] allMembers = range(memberCount);
        int[] allTopics = range(topicCount);
        int[][] subscribers = new int[topicCount][];
        Arrays.fill(subscribers, allMembers);
        int[][] memberTopics = new int[memberCount][];
        Arrays.fill(memberTopics, allTopics);
        return new Subscriptions(subscribers, memberTopics);
    }

    private Subscriptions heterogeneousSubscriptions(GroupSpec groupSpec) {
        int[][] memberTopics = memberTopics(groupSpec);
        return new Subscriptions(invert(memberTopics, topicCount), memberTopics);
    }

    private record CohortIndex(int[] baseLoad, int[] size, int[] topicCohortStart, int[] topicCohorts, int[][] rackSubscribers) { }

    /**
     * Computes the base load and size of every cohort, the cohorts of every topic and, when racks
     * are in use, the number of subscribers of every topic in every rack.
     */
    private CohortIndex indexCohorts() {
        int[] baseLoad = new int[cohortCount];
        int[] size = new int[cohortCount];
        int[] topicCohortStart = new int[topicCount + 1];
        int[][] rackSubscribers = usesRacks ? new int[topicCount][rackCount] : null;
        for (int m = 0; m < memberCount; m++) {
            size[memberCohort[m]]++;
        }
        for (int c = 0; c < cohortCount; c++) {
            for (int t : cohortTopics[c]) {
                baseLoad[c] += basePartitionCount[t];
                topicCohortStart[t + 1]++;
                if (rackSubscribers != null) {
                    rackSubscribers[t][cohortRack[c]] += size[c];
                }
            }
        }
        for (int t = 0; t < topicCount; t++) {
            topicCohortStart[t + 1] += topicCohortStart[t];
        }
        int[] topicCohorts = new int[topicCohortStart[topicCount]];
        int[] fill = Arrays.copyOf(topicCohortStart, topicCount);
        for (int c = 0; c < cohortCount; c++) {
            for (int t : cohortTopics[c]) {
                topicCohorts[fill[t]++] = c;
            }
        }
        return new CohortIndex(baseLoad, size, topicCohortStart, topicCohorts, rackSubscribers);
    }

    private record Backed(int[] start, int[] topics, long[] bits) { }

    /**
     * Indexes, per member, the topics of which it holds more than the base partitions, and sets
     * their bits when the group uses bitsets.
     */
    private Backed backed() {
        int[] start = new int[memberCount + 1];
        for (int t = 0; t < topicCount; t++) {
            for (int i = holderStart[t]; i < holderStart[t + 1]; i++) {
                if (holderValidCount[i] > basePartitionCount[t]) {
                    start[holderMember[i] + 1]++;
                }
            }
        }
        for (int m = 0; m < memberCount; m++) {
            start[m + 1] += start[m];
        }
        int[] topics = new int[start[memberCount]];
        long[] bits = usesBitsets ? newBitset() : null;
        int[] fill = Arrays.copyOf(start, memberCount);
        for (int t = 0; t < topicCount; t++) {
            for (int i = holderStart[t]; i < holderStart[t + 1]; i++) {
                if (holderValidCount[i] > basePartitionCount[t]) {
                    int m = holderMember[i];
                    topics[fill[m]++] = t;
                    if (bits != null) {
                        int bit = bitIndex(m, t);
                        bits[bit >>> 6] |= 1L << bit;
                    }
                }
            }
        }
        return new Backed(start, topics, bits);
    }

    private record Racks(int count, int[] memberRack, long[][] partitionRacks, int[][] supply) {
        static final Racks NONE = new Racks(0, null, null, null);
    }

    /**
     * @return The racks when rack awareness is in use, {@link Racks#NONE} otherwise.
     */
    private Racks racks(GroupSpec groupSpec, SubscribedTopicDescriber describer, boolean rackAwareEnabled) {
        if (!rackAwareEnabled) {
            return Racks.NONE;
        }
        Map<String, Integer> rackIndex = new HashMap<>();
        int[] racks = new int[memberCount];
        for (int m = 0; m < memberCount; m++) {
            Optional<String> rackId = groupSpec.memberSubscription(memberIds[m]).rackId();
            if (rackId.isEmpty()) {
                return Racks.NONE;
            }
            racks[m] = rackIndex.computeIfAbsent(rackId.get(), k -> rackIndex.size());
        }
        int count = rackIndex.size();
        // Rack awareness cannot change anything when all members are in the same rack.
        if (count < 2 || count > MAX_RACKS) {
            return Racks.NONE;
        }
        long[][] partitionRacks = new long[topicCount][];
        int[][] supply = new int[topicCount][count];
        for (int t = 0; t < topicCount; t++) {
            long[] masks = new long[partitionCounts[t]];
            for (int p = 0; p < masks.length; p++) {
                long mask = 0;
                for (String rack : describer.racksForPartition(topicIds[t], p)) {
                    Integer index = rackIndex.get(rack);
                    if (index != null) {
                        mask |= 1L << index;
                    }
                }
                masks[p] = mask;
                while (mask != 0) {
                    supply[t][Long.numberOfTrailingZeros(mask)]++;
                    mask &= mask - 1;
                }
            }
            partitionRacks[t] = masks;
        }
        return new Racks(count, racks, partitionRacks, supply);
    }

    private record Cohorts(int count, int[] memberCohort, int[] rack, int[][] topics) { }

    /**
     * With a single subscription, there is one cohort, or one per rack when racks are in use,
     * in which case the cohort of a rack has the index of the rack.
     */
    private Cohorts homogeneousCohorts() {
        int count = usesRacks ? rackCount : 1;
        int[] memberCohort = new int[memberCount];
        int[] rack = new int[count];
        int[][] topics = new int[count][];
        int[] allTopics = memberTopics[0];
        for (int c = 0; c < count; c++) {
            rack[c] = c;
            topics[c] = allTopics;
        }
        for (int m = 0; m < memberCount; m++) {
            memberCohort[m] = usesRacks ? memberRack[m] : 0;
        }
        return new Cohorts(count, memberCohort, rack, topics);
    }

    private Cohorts heterogeneousCohorts() {
        Map<CohortKey, Integer> cohortIndex = new HashMap<>();
        List<int[]> topics = new ArrayList<>();
        IntList racks = new IntList(16);
        int[] memberCohort = new int[memberCount];
        for (int m = 0; m < memberCount; m++) {
            int rack = usesRacks ? memberRack[m] : 0;
            CohortKey key = new CohortKey(memberTopics[m], rack);
            Integer c = cohortIndex.get(key);
            if (c == null) {
                c = cohortIndex.size();
                cohortIndex.put(key, c);
                topics.add(memberTopics[m]);
                racks.add(rack);
            }
            memberCohort[m] = c;
        }
        return new Cohorts(cohortIndex.size(), memberCohort, racks.toArray(), topics.toArray(new int[0][]));
    }

    private record CohortKey(int[] topics, int rack) {
        @Override
        public boolean equals(Object o) {
            return o instanceof CohortKey other && rack == other.rack && Arrays.equals(topics, other.topics);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(topics) + rack;
        }
    }

    private record Holders(int[] start, int[] member, Set<Integer>[] partitions, int[] validCount) { }

    /**
     * Indexes the current partitions by topic. Entries of topics that are gone or no longer
     * subscribed by the member, and empty entries, are dropped and the member is flagged.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Holders holders(GroupSpec groupSpec) {
        int maxEntries = 0;
        for (int m = 0; m < memberCount; m++) {
            Map<Uuid, Set<Integer>> currentAssignment = groupSpec.memberAssignment(memberIds[m]).partitions();
            currentAssignments[m] = currentAssignment;
            maxEntries += currentAssignment.size();
        }
        IntList entryMember = new IntList(Math.max(16, maxEntries));
        IntList entryTopic = new IntList(Math.max(16, maxEntries));
        List<Set<Integer>> entryPartitions = new ArrayList<>(Math.max(16, maxEntries));
        for (int m = 0; m < memberCount; m++) {
            for (Map.Entry<Uuid, Set<Integer>> entry : currentAssignments[m].entrySet()) {
                int t = topicIndex.indexOf(entry.getKey());
                if (t == TopicIndex.NONE || !isSubscribed(m, t) || entry.getValue().isEmpty()) {
                    hasDroppedPartitions[m] = true;
                    continue;
                }
                entryMember.add(m);
                entryTopic.add(t);
                entryPartitions.add(entry.getValue());
            }
        }

        int entryCount = entryMember.size();
        int[] start = new int[topicCount + 1];
        for (int i = 0; i < entryCount; i++) {
            start[entryTopic.get(i) + 1]++;
        }
        for (int t = 0; t < topicCount; t++) {
            start[t + 1] += start[t];
        }
        int[] member = new int[entryCount];
        Set<Integer>[] partitions = (Set<Integer>[]) new Set[entryCount];
        int[] validCount = new int[entryCount];
        int[] fill = Arrays.copyOf(start, topicCount);
        for (int i = 0; i < entryCount; i++) {
            int t = entryTopic.get(i);
            Set<Integer> current = entryPartitions.get(i);
            member[fill[t]] = entryMember.get(i);
            partitions[fill[t]] = current;
            validCount[fill[t]] = countValid(current, partitionCounts[t]);
            fill[t]++;
        }
        return new Holders(start, member, partitions, validCount);
    }

    /**
     * @return How many of the partitions have an id within the partition count.
     */
    private static int countValid(Set<Integer> partitions, int partitionCount) {
        int count = 0;
        for (int p : partitions) {
            if (p >= 0 && p < partitionCount) {
                count++;
            }
        }
        return count;
    }
}
