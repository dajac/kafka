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

import java.util.Arrays;

/**
 * Which member gets each extra partition of each topic.
 *
 * <p>The relation is kept both per topic, the receivers of its extra partitions, and per member,
 * the topics of which it gets an extra partition in ascending order. Whether a member gets an
 * extra partition of a topic is answered by a bitset over the members and topics, see
 * {@link GroupModel#newBitset}. The per member count of free extra partitions,
 * those not backed by a current partition, is maintained too. When racks are in use, the number
 * of extra partitions per topic and rack is also maintained.
 */
final class ExtraPartitions {
    private final GroupModel model;
    private final GroupModel.Racks racks;
    /**
     * Per topic, the row of its receivers in {@link #receivers}, sized to its extra partitions.
     */
    private final int[] receiverStart;
    /**
     * The receivers of the extra partitions of every topic, in no particular order.
     */
    private final int[] receivers;
    /**
     * Per topic, the number of extra partitions with a receiver.
     */
    private final int[] receiverCount;
    /**
     * Per member, the topics of which it gets an extra partition, ascending, up to its count.
     */
    private final int[][] topicsPerMember;
    /**
     * Per member, its number of extra partitions.
     */
    private final int[] countPerMember;
    /**
     * Per member, its number of free extra partitions.
     */
    private final int[] freeCountPerMember;
    /**
     * Per topic and rack, the number of extra partitions of members of the rack, when racks are in
     * use.
     */
    private final int[][] countPerRack;
    /**
     * Per member and topic, whether the member gets an extra partition of the topic, indexed by
     * {@link GroupModel#bitIndex}.
     */
    private final long[] bits;

    ExtraPartitions(GroupModel model) {
        this.model = model;
        this.racks = model.racks();
        receiverStart = new int[model.topicCount() + 1];
        for (int t = 0; t < model.topicCount(); t++) {
            receiverStart[t + 1] = receiverStart[t] + model.extraPartitionCount()[t];
        }
        receivers = new int[receiverStart[model.topicCount()]];
        receiverCount = new int[model.topicCount()];
        topicsPerMember = new int[model.memberCount()][];
        countPerMember = new int[model.memberCount()];
        freeCountPerMember = new int[model.memberCount()];
        countPerRack = model.usesRacks() ? new int[model.topicCount()][racks.count()] : null;
        bits = model.newBitset();
    }

    /**
     * @return Whether the member gets an extra partition of the topic.
     */
    boolean has(int member, int topic) {
        int bit = model.bitIndex(member, topic);
        return (bits[bit >>> 6] & (1L << bit)) != 0;
    }

    /**
     * @return The allocation of the member for the topic: the base partitions, plus one if it
     *         gets an extra partition.
     */
    int allocation(int member, int topic) {
        return model.basePartitionCount()[topic] + (has(member, topic) ? 1 : 0);
    }

    /**
     * Gives an extra partition of the topic to the member, which must not have one yet.
     *
     * @throws IllegalStateException If every extra partition of the topic already has a receiver.
     */
    void add(int member, int topic) {
        if (receiverCount[topic] == model.extraPartitionCount()[topic]) {
            throw new IllegalStateException("Every extra partition of topic " + model.topicIds()[topic] + " has a receiver");
        }
        receivers[receiverStart[topic] + receiverCount[topic]++] = member;
        if (!model.isBacked(member, topic)) {
            freeCountPerMember[member]++;
        }
        int[] topics = topicsPerMember[member];
        if (topics == null) {
            topics = new int[Math.max(4, model.backedCount(member) + 1)];
            topicsPerMember[member] = topics;
        } else if (countPerMember[member] == topics.length) {
            topics = Arrays.copyOf(topics, topics.length * 2);
            topicsPerMember[member] = topics;
        }
        // Keep the topics sorted. The common case appends, since topics are processed in order.
        int count = countPerMember[member]++;
        int at = count;
        while (at > 0 && topics[at - 1] > topic) {
            at--;
        }
        System.arraycopy(topics, at, topics, at + 1, count - at);
        topics[at] = topic;
        int bit = model.bitIndex(member, topic);
        bits[bit >>> 6] |= 1L << bit;
        if (countPerRack != null) {
            countPerRack[topic][racks.memberRack()[member]]++;
        }
    }

    /**
     * Takes the extra partition of the topic back from the member, which must have one.
     */
    void remove(int member, int topic) {
        int start = receiverStart[topic];
        int end = start + receiverCount[topic];
        for (int i = start; i < end; i++) {
            if (receivers[i] == member) {
                receivers[i] = receivers[end - 1];
                break;
            }
        }
        receiverCount[topic]--;
        int[] topics = topicsPerMember[member];
        int count = countPerMember[member];
        int at = Arrays.binarySearch(topics, 0, count, topic);
        System.arraycopy(topics, at + 1, topics, at, count - at - 1);
        countPerMember[member]--;
        int bit = model.bitIndex(member, topic);
        bits[bit >>> 6] &= ~(1L << bit);
        if (!model.isBacked(member, topic)) {
            freeCountPerMember[member]--;
        }
        if (countPerRack != null) {
            countPerRack[topic][racks.memberRack()[member]]--;
        }
    }

    /**
     * @return The number of extra partitions of the topic that have a receiver.
     */
    int receiverCount(int topic) {
        return receiverCount[topic];
    }

    /**
     * @return The {@code i}-th receiver of an extra partition of the topic.
     */
    int receiverAt(int topic, int i) {
        return receivers[receiverStart[topic] + i];
    }

    /**
     * Sorts the receivers of the topic by ascending member index, for deterministic iteration.
     */
    void sortReceivers(int topic) {
        Arrays.sort(receivers, receiverStart[topic], receiverStart[topic] + receiverCount[topic]);
    }

    /**
     * @return The number of extra partitions of the member.
     */
    int countOf(int member) {
        return countPerMember[member];
    }

    /**
     * @return The topics of which the member gets an extra partition, ascending. Only the first
     *         {@link #countOf} entries are valid, the array is live, and it is null while the
     *         member has no extra partition.
     */
    int[] topicsOf(int member) {
        return topicsPerMember[member];
    }

    /**
     * @return The number of extra partitions of the member that are not backed by a current
     *         partition.
     */
    int freeCountOf(int member) {
        return freeCountPerMember[member];
    }

    /**
     * @return The number of extra partitions of the topic given to members of the rack.
     */
    int countInRack(int topic, int rack) {
        return countPerRack[topic][rack];
    }
}
