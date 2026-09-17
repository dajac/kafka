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
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntArrayList;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.UuidIndex;

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
 * by index, and every array indexed by member or topic follows that numbering. A list per member
 * or per topic is stored as the rows of one flat array: the entries of item {@code i} are the
 * values from {@code start[i]} inclusive to {@code start[i + 1]} exclusive, in an array of
 * starts with one more entry than items.
 *
 * <p>The model is built once by its constructor and read by every phase through its accessors.
 * The arrays are shared, not copied, and nobody writes into them once the model is built.
 */
final class GroupModel {
    /**
     * Marks the absence of a member, topic or partition index.
     */
    static final int NONE = -1;

    /**
     * Rack awareness is used when the members of the group are in at most this many distinct
     * racks, the racks having a replica of a partition being kept as one bit per rack in a long.
     * Above it, the racks are ignored for the assignment.
     */
    static final int MAX_RACKS = 64;

    /**
     * The number of members, {@code N}. Members are numbered from 0 to {@code N - 1} in the sorted
     * order of their ids.
     */
    private final int memberCount;

    /**
     * Per member, its id: length {@code N}, sorted, the index of a member being its position.
     */
    private final String[] memberIds;

    /**
     * Per member, its current assignment as given in the input: length {@code N}, the very map
     * of the input, never null.
     */
    private final Map<Uuid, Set<Integer>>[] currentAssignments;

    /**
     * Per member, whether some entries of its current assignment are stale: length {@code N}.
     * An entry is stale when its topic no longer exists or is no longer subscribed by the
     * member, or when it is empty. Such a member cannot get its current assignment instance
     * back as is.
     */
    private final boolean[] hasStalePartitions;

    /**
     * The number of subscribed topics, {@code T}. Topics are numbered from 0 to {@code T - 1} in
     * the sorted order of their ids.
     */
    private final int topicCount;

    /**
     * Per topic, its id: length {@code T}, sorted, the index of a topic being its position.
     */
    private final Uuid[] topicIds;

    /**
     * The index of the topics by id, giving the topic of an entry of a current assignment.
     */
    private final UuidIndex topicIndex;

    /**
     * Per topic, its number of partitions: length {@code T}, possibly zero.
     */
    private final int[] partitionCounts;

    /**
     * Per topic, its base partitions: length {@code T}, the partition count divided by the
     * number of subscribers, which every subscriber gets.
     */
    private final int[] basePartitionCount;

    /**
     * Per topic, its number of extra partitions: length {@code T}, the partition count modulo
     * the number of subscribers, each going to a distinct subscriber.
     */
    private final int[] extraPartitionCount;

    /**
     * Whether all members have the same subscription.
     */
    private final boolean homogeneous;

    /**
     * The subscriptions, per topic and per member.
     */
    private final Subscriptions subscriptions;

    /**
     * The current owners of the partitions of every topic.
     */
    private final Owners owners;

    /**
     * Per member, the topics of which it currently owns more than the base partitions.
     */
    private final Backed backed;

    /**
     * The cohorts: groups of members with the same subscription, and the same rack when racks
     * are in use.
     */
    private final Cohorts cohorts;

    /**
     * The racks of the members and of the partition replicas, {@link Racks#NONE} when rack
     * awareness is not in use.
     */
    private final Racks racks;

    @SuppressWarnings({"unchecked", "rawtypes"})
    GroupModel(
        GroupSpec groupSpec,
        SubscribedTopicDescriber describer,
        boolean rackAwareEnabled
    ) {
        homogeneous = groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS;

        memberIds = groupSpec.memberIds().toArray(new String[0]);
        Arrays.sort(memberIds);
        memberCount = memberIds.length;

        topicIds = subscribedTopicIds(groupSpec);
        topicCount = topicIds.length;
        topicIndex = new UuidIndex(topicIds);
        partitionCounts = partitionCounts(describer);

        subscriptions = homogeneous ? homogeneousSubscriptions() : heterogeneousSubscriptions(groupSpec);
        basePartitionCount = new int[topicCount];
        extraPartitionCount = new int[topicCount];
        for (int t = 0; t < topicCount; t++) {
            int subscriberCount = subscriptions.subscribers()[t].length;
            basePartitionCount[t] = partitionCounts[t] / subscriberCount;
            extraPartitionCount[t] = partitionCounts[t] % subscriberCount;
        }

        racks = racks(groupSpec, describer, rackAwareEnabled);
        cohorts = indexCohorts(homogeneous ? homogeneousCohorts() : heterogeneousCohorts());

        currentAssignments = (Map<Uuid, Set<Integer>>[]) new Map[memberCount];
        hasStalePartitions = new boolean[memberCount];
        owners = owners(groupSpec);
        backed = backed();
    }

    /**
     * @return The number of members.
     */
    int memberCount() {
        return memberCount;
    }

    /**
     * @return The member ids, sorted, the index of a member being its position here.
     */
    String[] memberIds() {
        return memberIds;
    }

    /**
     * @return Per member, its current assignment as given in the input.
     */
    Map<Uuid, Set<Integer>>[] currentAssignments() {
        return currentAssignments;
    }

    /**
     * @return Per member, whether some entries of its current assignment are stale.
     */
    boolean[] hasStalePartitions() {
        return hasStalePartitions;
    }

    /**
     * @return The number of subscribed topics.
     */
    int topicCount() {
        return topicCount;
    }

    /**
     * @return The topic ids, sorted, the index of a topic being its position here.
     */
    Uuid[] topicIds() {
        return topicIds;
    }

    /**
     * @return The index of the topics by id.
     */
    UuidIndex topicIndex() {
        return topicIndex;
    }

    /**
     * @return Per topic, its number of partitions.
     */
    int[] partitionCounts() {
        return partitionCounts;
    }

    /**
     * @return Per topic, its base partitions: the number every subscriber gets.
     */
    int[] basePartitionCount() {
        return basePartitionCount;
    }

    /**
     * @return Per topic, its number of extra partitions, each going to a distinct subscriber.
     */
    int[] extraPartitionCount() {
        return extraPartitionCount;
    }

    /**
     * @return Whether all members have the same subscription.
     */
    boolean homogeneous() {
        return homogeneous;
    }

    /**
     * @return Per topic, its subscribers in ascending order.
     */
    int[][] subscribers() {
        return subscriptions.subscribers();
    }

    /**
     * @return Per member, its topics in ascending order.
     */
    int[][] memberTopics() {
        return subscriptions.memberTopics();
    }

    /**
     * @return The current owners of the partitions of every topic.
     */
    Owners owners() {
        return owners;
    }

    /**
     * @return The cohorts of the group.
     */
    Cohorts cohorts() {
        return cohorts;
    }

    /**
     * @return The racks of the members and partitions, or {@link Racks#NONE} when rack awareness
     *         is not in use.
     */
    Racks racks() {
        return racks;
    }

    /**
     * @return Whether rack awareness is in use: enabled, every member has a rack and the members
     *         are in 2 to {@link #MAX_RACKS} racks.
     */
    boolean usesRacks() {
        return racks.count() > 0;
    }

    /**
     * @return Whether the member is subscribed to the topic.
     */
    boolean isSubscribed(int member, int topic) {
        return homogeneous || Arrays.binarySearch(subscriptions.memberTopics()[member], topic) >= 0;
    }

    /**
     * @return Whether the member currently owns more partitions of the topic than the base
     *         partitions, so that an extra partition of the topic lets it keep one of them.
     */
    boolean isBacked(int member, int topic) {
        int bit = bitIndex(member, topic);
        return (backed.bits()[bit >>> 6] & (1L << bit)) != 0;
    }

    /**
     * @return The number of topics of which the member currently owns more than the base
     *         partitions.
     */
    int backedCount(int member) {
        return backed.counts()[member];
    }

    /**
     * @return A cleared bitset with one bit per topic and member, indexed by {@link #bitIndex}:
     *         a word per 64 pairs, so 12.5 MB for 10,000 members subscribed to 10,000 topics,
     *         and a few kilobytes for the groups commonly seen.
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
            max = Math.max(max, cohorts.topicCohortStart()[t + 1] - cohorts.topicCohortStart()[t]);
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
        Uuid[] sorted;
        if (homogeneous) {
            sorted = groupSpec.memberSubscription(memberIds[0]).subscribedTopicIds().toArray(new Uuid[0]);
        } else {
            Set<Uuid> topicSet = new HashSet<>();
            for (String memberId : memberIds) {
                topicSet.addAll(groupSpec.memberSubscription(memberId).subscribedTopicIds());
            }
            sorted = topicSet.toArray(new Uuid[0]);
        }
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
        IntArrayList buffer = new IntArrayList(16);
        for (int m = 0; m < memberCount; m++) {
            buffer.clear();
            for (Uuid topicId : groupSpec.memberSubscription(memberIds[m]).subscribedTopicIds()) {
                int t = topicIndex.indexOf(topicId);
                if (t != UuidIndex.NONE) {
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

    /**
     * The subscriptions, both ways.
     *
     * @param subscribers   Per topic, its subscribers in ascending member order: length {@code T}.
     *                      Every topic shares one array of all the members when homogeneous.
     * @param memberTopics  Per member, its topics in ascending order: length {@code N}. Every
     *                      member shares one array of all the topics when homogeneous.
     */
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

    /**
     * The cohorts of the group: groups of members sharing a subscription, and a rack when racks
     * are in use, numbered from zero.
     *
     * @param count             The number of cohorts.
     * @param memberCohort      Per member, its cohort.
     * @param rack              Per cohort, its rack, or zero when racks are not in use.
     * @param baseLoad          Per cohort, the base load of its members: the sum of the base
     *                          partitions of their topics.
     * @param size              Per cohort, its number of members.
     * @param topics            Per cohort, its topics in ascending order.
     * @param topicCohortStart  Per topic, the start of its row in {@code topicCohorts}.
     * @param topicCohorts      The cohorts subscribed to every topic, by rows.
     * @param rackSubscribers   Per topic and rack, the number of subscribers of the topic in the
     *                          rack, when racks are in use.
     */
    record Cohorts(
        int count,
        int[] memberCohort,
        int[] rack,
        int[] baseLoad,
        int[] size,
        int[][] topics,
        int[] topicCohortStart,
        int[] topicCohorts,
        int[][] rackSubscribers
    ) { }

    /**
     * Computes the base load and size of every cohort, the cohorts of every topic and, when racks
     * are in use, the number of subscribers of every topic in every rack.
     */
    private Cohorts indexCohorts(CohortGroups groups) {
        int count = groups.count();
        int[] baseLoad = new int[count];
        int[] size = new int[count];
        int[] topicCohortStart = new int[topicCount + 1];
        int[][] rackSubscribers = racks.count() > 0 ? new int[topicCount][racks.count()] : null;
        for (int m = 0; m < memberCount; m++) {
            size[groups.memberCohort()[m]]++;
        }
        for (int c = 0; c < count; c++) {
            for (int t : groups.topics()[c]) {
                baseLoad[c] += basePartitionCount[t];
                topicCohortStart[t + 1]++;
                if (rackSubscribers != null) {
                    rackSubscribers[t][groups.rack()[c]] += size[c];
                }
            }
        }
        for (int t = 0; t < topicCount; t++) {
            topicCohortStart[t + 1] += topicCohortStart[t];
        }
        int[] topicCohorts = new int[topicCohortStart[topicCount]];
        int[] fill = Arrays.copyOf(topicCohortStart, topicCount);
        for (int c = 0; c < count; c++) {
            for (int t : groups.topics()[c]) {
                topicCohorts[fill[t]++] = c;
            }
        }
        return new Cohorts(count, groups.memberCohort(), groups.rack(), baseLoad, size, groups.topics(), topicCohortStart, topicCohorts, rackSubscribers);
    }

    /**
     * The topics of which the members currently own more than the base partitions.
     *
     * @param counts    Per member, the number of such topics: length {@code N}.
     * @param bits      Per member and topic, whether the member owns more than the base
     *                  partitions of the topic, indexed by {@link #bitIndex}.
     */
    private record Backed(int[] counts, long[] bits) { }

    /**
     * Counts, per member, the topics of which it owns more than the base partitions, and sets
     * their bits.
     */
    private Backed backed() {
        int[] counts = new int[memberCount];
        long[] bits = newBitset();
        for (int t = 0; t < topicCount; t++) {
            for (int i = owners.start()[t]; i < owners.start()[t + 1]; i++) {
                if (owners.validCount()[i] > basePartitionCount[t]) {
                    int m = owners.member()[i];
                    counts[m]++;
                    int bit = bitIndex(m, t);
                    bits[bit >>> 6] |= 1L << bit;
                }
            }
        }
        return new Backed(counts, bits);
    }

    /**
     * The racks of the members and of the partition replicas, numbered from zero, when rack
     * awareness is in use.
     *
     * @param count             The number of racks, zero when rack awareness is not in use.
     * @param memberRack        Per member, its rack.
     * @param partitionRacks    Per topic and partition, the racks having a replica, one bit per
     *                          rack.
     * @param supply            Per topic and rack, the number of partitions with a replica in
     *                          the rack.
     */
    record Racks(int count, int[] memberRack, long[][] partitionRacks, int[][] supply) {
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

    private record CohortGroups(int count, int[] memberCohort, int[] rack, int[][] topics) { }

    /**
     * With a single subscription, there is one cohort, or one per rack when racks are in use,
     * in which case the cohort of a rack has the index of the rack.
     */
    private CohortGroups homogeneousCohorts() {
        boolean usesRacks = racks.count() > 0;
        int count = usesRacks ? racks.count() : 1;
        int[] memberCohort = new int[memberCount];
        int[] rack = new int[count];
        int[][] topics = new int[count][];
        int[] allTopics = subscriptions.memberTopics()[0];
        for (int c = 0; c < count; c++) {
            rack[c] = c;
            topics[c] = allTopics;
        }
        for (int m = 0; m < memberCount; m++) {
            memberCohort[m] = usesRacks ? racks.memberRack()[m] : 0;
        }
        return new CohortGroups(count, memberCohort, rack, topics);
    }

    private CohortGroups heterogeneousCohorts() {
        boolean usesRacks = racks.count() > 0;
        Map<CohortKey, Integer> cohortIndex = new HashMap<>();
        List<int[]> topics = new ArrayList<>();
        IntArrayList cohortRacks = new IntArrayList(16);
        int[] memberCohort = new int[memberCount];
        for (int m = 0; m < memberCount; m++) {
            int rack = usesRacks ? racks.memberRack()[m] : 0;
            CohortKey key = new CohortKey(subscriptions.memberTopics()[m], rack);
            Integer c = cohortIndex.get(key);
            if (c == null) {
                c = cohortIndex.size();
                cohortIndex.put(key, c);
                topics.add(subscriptions.memberTopics()[m]);
                cohortRacks.add(rack);
            }
            memberCohort[m] = c;
        }
        return new CohortGroups(cohortIndex.size(), memberCohort, cohortRacks.toArray(), topics.toArray(new int[0][]));
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

    /**
     * The current owners of the partitions of every topic, one entry per member owning
     * partitions of a topic it is subscribed to, in ascending member order within a topic.
     *
     * @param start         Per topic, the start of its row in the other arrays.
     * @param member        Per entry, the member.
     * @param partitions    Per entry, the partitions of the topic the member currently owns, as
     *                      given in the input.
     * @param validCount    Per entry, how many of those partitions exist, that is have an id
     *                      below the partition count of the topic. The others are stale.
     */
    record Owners(int[] start, int[] member, Set<Integer>[] partitions, int[] validCount) { }

    /**
     * Indexes the current partitions by topic. Entries of topics that are gone or no longer
     * subscribed by the member, and empty entries, are stale and the member is flagged.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Owners owners(GroupSpec groupSpec) {
        int maxEntries = 0;
        for (int m = 0; m < memberCount; m++) {
            Map<Uuid, Set<Integer>> currentAssignment = groupSpec.memberAssignment(memberIds[m]).partitions();
            currentAssignments[m] = currentAssignment;
            maxEntries += currentAssignment.size();
        }
        IntArrayList entryMember = new IntArrayList(Math.max(16, maxEntries));
        IntArrayList entryTopic = new IntArrayList(Math.max(16, maxEntries));
        List<Set<Integer>> entryPartitions = new ArrayList<>(Math.max(16, maxEntries));
        for (int m = 0; m < memberCount; m++) {
            for (Map.Entry<Uuid, Set<Integer>> entry : currentAssignments[m].entrySet()) {
                int t = topicIndex.indexOf(entry.getKey());
                if (t == UuidIndex.NONE || !isSubscribed(m, t) || entry.getValue().isEmpty()) {
                    hasStalePartitions[m] = true;
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
        return new Owners(start, member, partitions, validCount);
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
