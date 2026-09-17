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

import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;

import java.util.Arrays;
import java.util.Set;

import static org.apache.kafka.coordinator.group.assignor.uniform2.GroupModel.NONE;

/**
 * The partition phase of {@link AssignmentBuilder}: chooses the partition ids every
 * member gets, one topic at a time, given the quotas.
 *
 * <p>Each current holder keeps its current partitions up to its quota, lowest ids first, and
 * releases the rest. The partitions nobody kept are handed out, in ascending order, to the
 * members below their quota: the current holders first, then the other subscribers, each in
 * ascending member order. A topic in which every holder already has exactly its quota, and the
 * holders together hold every partition, is emitted as is. A member whose partitions of a topic
 * did not change gets its current set back rather than a copy.
 *
 * <p>{@link RackAwarePartitionAssigner} replaces the per topic assignment when racks are
 * in use, through the {@link #isSettled} and {@link #assignTopic} hooks, and reuses the loop,
 * the deficits and the emission from this class.
 */
class PartitionAssigner {
    final GroupModel model;
    final ExtraPartitions extras;
    final TopicScratch scratch;
    private final AssignmentResult result;

    PartitionAssigner(GroupModel model, ExtraPartitions extras) {
        this.model = model;
        this.extras = extras;
        scratch = new TopicScratch(model.memberCount, model.maxPartitionsPerTopic());
        result = new AssignmentResult(model);
    }

    GroupAssignment assign() {
        for (int t = 0; t < model.topicCount; t++) {
            int partitionCount = model.partitionCounts[t];
            if (partitionCount == 0) {
                // Anything currently held in a topic without partitions is dropped.
                for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
                    result.markChanged(model.holderMember[i]);
                }
                continue;
            }
            if (isSettled(t)) {
                emitSettled(t);
                continue;
            }
            Arrays.fill(scratch.owner, 0, partitionCount, NONE);
            assignTopic(t);
            emit(t);
        }
        return result.build();
    }

    /**
     * Assigns every partition of the topic to a member in {@link TopicScratch#owner}.
     */
    void assignTopic(int t) {
        keepCurrentPartitions(t);
        computeDeficits(t);
        fillDeficits(t);
    }

    /**
     * A topic is settled when every current holder holds exactly its quota, all its current
     * partitions exist, and the holders together hold every partition, so that nothing has to
     * move and the topic is emitted as is. This relies on the current assignment being
     * consistent, every partition being held once, which the target assignment maintained by
     * the coordinator guarantees.
     */
    boolean isSettled(int t) {
        int held = 0;
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            int count = model.holderValidCount[i];
            if (count != model.holderPartitions[i].size() || count != extras.quota(model.holderMember[i], t)) {
                return false;
            }
            held += count;
        }
        return held == model.partitionCounts[t];
    }

    private void emitSettled(int t) {
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            result.add(model.holderMember[i], t, model.holderPartitions[i]);
        }
    }

    /**
     * Every current holder keeps its current partitions up to its quota, lowest ids first.
     */
    private void keepCurrentPartitions(int t) {
        int partitionCount = model.partitionCounts[t];
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            int m = model.holderMember[i];
            Set<Integer> current = model.holderPartitions[i];
            int quota = extras.quota(m, t);
            int count = 0;
            for (int p : current) {
                if (p >= 0 && p < partitionCount) {
                    scratch.partitions[count++] = p;
                }
            }
            if (count > quota) {
                Arrays.sort(scratch.partitions, 0, count);
                count = quota;
            }
            scratch.keep(m, current, count);
        }
    }

    /**
     * Records the deficit of every member that may receive partitions of the topic, in ascending
     * member order.
     */
    void computeDeficits(int t) {
        int count = receiverCount(t);
        for (int i = 0; i < count; i++) {
            recordDeficit(t, receiverAt(t, i));
        }
    }

    /**
     * The members that may receive partitions of the topic are all its subscribers, or only the
     * recipients of an extra partition when the base is zero. The recipients are sorted here, so
     * that {@link #receiverAt} follows member order in both cases.
     *
     * @return The number of members that may receive partitions of the topic.
     */
    int receiverCount(int t) {
        if (model.basePartitionCount[t] > 0) {
            return model.subscribers[t].length;
        }
        extras.sortRecipients(t);
        return extras.recipientCount(t);
    }

    /**
     * @return The {@code i}-th member that may receive partitions of the topic, see {@link #receiverCount}.
     */
    int receiverAt(int t, int i) {
        return model.basePartitionCount[t] > 0 ? model.subscribers[t][i] : extras.recipientAt(t, i);
    }

    private void recordDeficit(int t, int m) {
        int quota = extras.quota(m, t);
        int participant = scratch.participantOf(m);
        int kept = participant == NONE ? 0 : scratch.kept[participant];
        if (quota > kept) {
            scratch.deficit[scratch.participant(m)] = quota - kept;
        }
    }

    /**
     * The unassigned partitions, in ascending order, fill the deficits in participant order.
     * The quotas add up to the partition count, so the deficits and the unassigned partitions
     * match exactly unless a partition is currently held by several members, in which case
     * some partitions are left over.
     */
    private void fillDeficits(int t) {
        int partitionCount = model.partitionCounts[t];
        int next = 0;
        IntList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            int deficit = scratch.deficit[i];
            if (deficit == 0) {
                continue;
            }
            int m = participants.get(i);
            while (deficit > 0) {
                while (next < partitionCount && scratch.owner[next] != NONE) {
                    next++;
                }
                if (next == partitionCount) {
                    throw inconsistentAssignment(t);
                }
                scratch.owner[next++] = m;
                deficit--;
            }
            scratch.deficit[i] = 0;
        }
        while (next < partitionCount && scratch.owner[next] != NONE) {
            next++;
        }
        if (next < partitionCount) {
            throw inconsistentAssignment(t);
        }
    }

    /**
     * @return The exception for a current assignment in which a partition is held by several
     *         members, which the algorithm relies on never happening.
     */
    PartitionAssignorException inconsistentAssignment(int t) {
        return new PartitionAssignorException("The current assignment of topic " + model.topicIds[t]
            + " is inconsistent: a partition is held by several members.");
    }

    /**
     * Emits the partition sets of the topic. A member whose partitions did not change gets its
     * current set back, so that unchanged assignments can be returned as they are.
     */
    private void emit(int t) {
        int partitionCount = model.partitionCounts[t];
        IntList participants = scratch.participants;
        for (int p = 0; p < partitionCount; p++) {
            scratch.count[scratch.participant(scratch.owner[p])]++;
        }
        // Counting sort of the partitions by participant: they stay sorted within a member.
        int total = participants.size();
        int[] offsets = new int[total + 1];
        for (int i = 0; i < total; i++) {
            offsets[i + 1] = offsets[i] + scratch.count[i];
        }
        int[] fill = Arrays.copyOf(offsets, total);
        for (int p = 0; p < partitionCount; p++) {
            scratch.partitions[fill[scratch.participantOf(scratch.owner[p])]++] = p;
        }
        for (int i = 0; i < total; i++) {
            int m = participants.get(i);
            int count = scratch.count[i];
            if (count > 0) {
                if (canReuse(i, offsets[i], count)) {
                    result.add(m, t, scratch.currentPartitions[i]);
                } else {
                    result.add(m, t, new IntArraySet(Arrays.copyOfRange(scratch.partitions, offsets[i], offsets[i] + count)));
                    result.markChanged(m);
                }
            } else if (scratch.currentPartitions[i] != null) {
                result.markChanged(m);
            }
        }
        scratch.clear();
    }

    /**
     * @return Whether the new partitions of the participant are exactly its current ones.
     */
    private boolean canReuse(int participant, int offset, int count) {
        Set<Integer> current = scratch.currentPartitions[participant];
        if (current == null || current.size() != count) {
            return false;
        }
        if (scratch.keptAll[participant]) {
            return true;
        }
        for (int j = offset; j < offset + count; j++) {
            if (!current.contains(scratch.partitions[j])) {
                return false;
            }
        }
        return true;
    }

    /**
     * The working state of the partition phase for the topic at hand, sized once for the largest
     * topic and reused from topic to topic.
     *
     * <p>The members involved in a topic, its current holders and the subscribers receiving
     * partitions, are numbered in order of appearance and called participants. The per participant
     * arrays are only valid up to the number of participants and are reset by {@link #participant}
     * when a participant is added, so {@link #clear} only has to forget the participants.
     */
    static final class TopicScratch {
        /** Per partition, the member it is assigned to, or NONE. */
        final int[] owner;
        /** A buffer of partition ids. */
        final int[] partitions;
        /** The participants, in order of appearance. */
        final IntList participants;
        /** Per member, its participant index, or NONE. */
        private final int[] participantOfMember;
        /** Per participant, the number of its current partitions it keeps. */
        final int[] kept;
        /** Per participant, the number of partitions it still has to receive. */
        final int[] deficit;
        /** Per participant, the number of partitions assigned to it, counted when emitting. */
        final int[] count;
        /**
         * Per participant, whether its final partitions are exactly its current ones: set when it
         * keeps every one of its current partitions, and cleared by anything that moves a kept
         * partition afterwards, so that emitting can return its current set without comparing.
         */
        final boolean[] keptAll;
        /** Per participant, its current partitions, or null when it holds none. */
        final Set<Integer>[] currentPartitions;

        @SuppressWarnings({"unchecked", "rawtypes"})
        TopicScratch(int memberCount, int maxPartitions) {
            owner = new int[maxPartitions];
            partitions = new int[maxPartitions];
            participants = new IntList(16);
            participantOfMember = new int[memberCount];
            Arrays.fill(participantOfMember, NONE);
            // Every member may be involved in a topic, as a current holder or as a receiver.
            kept = new int[memberCount];
            deficit = new int[memberCount];
            count = new int[memberCount];
            keptAll = new boolean[memberCount];
            currentPartitions = (Set<Integer>[]) new Set[memberCount];
        }

        /**
         * @return The participant index of the member, adding it as a participant if needed.
         */
        int participant(int member) {
            int participant = participantOfMember[member];
            if (participant == NONE) {
                participant = participants.size();
                participants.add(member);
                participantOfMember[member] = participant;
                kept[participant] = 0;
                deficit[participant] = 0;
                count[participant] = 0;
                keptAll[participant] = false;
                currentPartitions[participant] = null;
            }
            return participant;
        }

        /**
         * @return The participant index of the member, or NONE if it is not a participant.
         */
        int participantOf(int member) {
            return participantOfMember[member];
        }

        /**
         * Records that the member keeps the first {@code keptCount} partitions of the buffer among
         * its current ones, and assigns them to it.
         */
        void keep(int member, Set<Integer> current, int keptCount) {
            int participant = participant(member);
            for (int j = 0; j < keptCount; j++) {
                owner[partitions[j]] = member;
            }
            kept[participant] = keptCount;
            keptAll[participant] = keptCount == current.size();
            currentPartitions[participant] = current;
        }

        /**
         * Forgets the participants, ready for the next topic.
         */
        void clear() {
            for (int i = 0; i < participants.size(); i++) {
                participantOfMember[participants.get(i)] = NONE;
            }
            participants.clear();
        }
    }
}
