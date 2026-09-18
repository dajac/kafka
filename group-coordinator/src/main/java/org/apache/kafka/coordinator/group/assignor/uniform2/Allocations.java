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
 * The allocations of the members for the topics. Every subscriber of a topic gets its base
 * partitions, so the allocations follow from which subscribers get the extra partitions, which
 * is what is recorded here, see {@link #allocation}.
 *
 * <p>The relation is kept both per topic, the receivers of its extra partitions, and per member,
 * the topics of which it gets an extra partition in ascending order. Whether a member gets an
 * extra partition of a topic is answered by a bitset over the members and topics, see
 * {@link GroupModel#newBitset}. The per member count of free extra partitions,
 * those not backed by a current partition, is maintained too.
 */
final class Allocations {
    private final GroupModel model;
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
     * Per member and topic, whether the member gets an extra partition of the topic, indexed by
     * {@link GroupModel#bitIndex}.
     */
    private final long[] bits;

    Allocations(GroupModel model) {
        this.model = model;
        receiverStart = new int[model.topicCount() + 1];
        for (int t = 0; t < model.topicCount(); t++) {
            receiverStart[t + 1] = receiverStart[t] + model.extraPartitionCount()[t];
        }
        receivers = new int[receiverStart[model.topicCount()]];
        receiverCount = new int[model.topicCount()];
        topicsPerMember = new int[model.memberCount()][];
        countPerMember = new int[model.memberCount()];
        freeCountPerMember = new int[model.memberCount()];
        bits = model.newBitset();
    }

    /**
     * @return Whether the member gets an extra partition of the topic.
     */
    boolean hasExtra(int member, int topic) {
        int bit = model.bitIndex(member, topic);
        return (bits[bit >>> 6] & (1L << bit)) != 0;
    }

    /**
     * @return The allocation of the member for the topic: the base partitions, plus one if it
     *         gets an extra partition.
     */
    int allocation(int member, int topic) {
        return model.basePartitionCount()[topic] + (hasExtra(member, topic) ? 1 : 0);
    }

    /**
     * Gives an extra partition of the topic to the member, which must not have one yet.
     *
     * @throws IllegalStateException If every extra partition of the topic already has a receiver.
     */
    void addExtra(int member, int topic) {
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
    }

    /**
     * Takes the extra partition of the topic back from the member, which must have one.
     */
    void removeExtra(int member, int topic) {
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
    }

    /**
     * @return The number of extra partitions of the topic that have a receiver.
     */
    int extraReceiverCount(int topic) {
        return receiverCount[topic];
    }

    /**
     * @return The {@code i}-th receiver of an extra partition of the topic.
     */
    int extraReceiverAt(int topic, int i) {
        return receivers[receiverStart[topic] + i];
    }

    /**
     * Sorts the receivers of the topic by ascending member index, for deterministic iteration.
     */
    void sortExtraReceivers(int topic) {
        Arrays.sort(receivers, receiverStart[topic], receiverStart[topic] + receiverCount[topic]);
    }

    /**
     * @return The number of extra partitions of the member.
     */
    int extraCount(int member) {
        return countPerMember[member];
    }

    /**
     * @return The topics of which the member gets an extra partition, ascending. Only the first
     *         {@link #extraCount} entries are valid, the array is live, and it is null while the
     *         member has no extra partition.
     */
    int[] topicsOf(int member) {
        return topicsPerMember[member];
    }

    /**
     * @return The number of extra partitions of the member that are not backed by a current
     *         partition.
     */
    int freeExtraCount(int member) {
        return freeCountPerMember[member];
    }
}
