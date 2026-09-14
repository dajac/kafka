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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.Uniform2GroupModel.NONE;

/**
 * The partition phase when racks are in use, see {@link Uniform2AssignmentBuilder}. The quotas
 * are the same as without racks; only the choice of partition ids changes, so that as many
 * members as possible get partitions having a replica in their rack. For each topic:
 * <ol>
 *     <li><b>Keep:</b> each current holder keeps its current aligned partitions up to its quota.
 *     Misaligned partitions are released so that they can be realigned. A holder with more
 *     aligned partitions than its quota releases first the ones most useful to the racks whose
 *     members are below their quotas, then the ones with the most replica racks.</li>
 *     <li><b>Align:</b> the released partitions are handed to the members below their quota
 *     with a maximum flow from replica rack sets to racks, see {@link Uniform2MaxFlow}.</li>
 *     <li><b>Leftovers:</b> the partitions that cannot be aligned go back to their previous
 *     holder if it is still below its quota, then to the remaining members below their quota,
 *     the current holders of the topic first, then its other subscribers, each in member id
 *     order.</li>
 *     <li><b>Swap:</b> each partition still misaligned is swapped with a partition held by a
 *     member in one of its replica racks, when that partition has a replica in the rack of the
 *     misaligned holder.</li>
 * </ol>
 * A settled topic is only emitted as is when all its partitions are aligned, since it may
 * otherwise be realigned by swaps.
 */
final class Uniform2RackAwarePartitionAssigner extends Uniform2PartitionAssigner {
    /**
     * The flow network takes one node per distinct replica rack set among the released
     * partitions. Beyond this many, the remaining partitions are treated as leftovers, which
     * bounds the size of the network with many racks and replicas.
     */
    private static final int MAX_FLOW_GROUPS = 1024;

    /** Per partition of the topic at hand, the member currently holding it, or NONE. */
    private final int[] previousOwner;

    Uniform2RackAwarePartitionAssigner(Uniform2GroupModel model, Uniform2ExtraPartitions extras) {
        super(model, extras);
        previousOwner = new int[model.maxPartitionsPerTopic()];
    }

    /**
     * A settled topic is also required to have all its partitions aligned, as the swap step
     * could otherwise improve it.
     */
    @Override
    boolean isSettled(int t) {
        if (!super.isSettled(t)) {
            return false;
        }
        long[] racks = model.partitionRacks[t];
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            long rackBit = 1L << model.memberRack[model.holderMember[i]];
            for (int p : model.holderPartitions[i]) {
                if ((racks[p] & rackBit) == 0) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    void assignTopic(int t) {
        Arrays.fill(previousOwner, 0, model.partitionCounts[t], NONE);
        keepAlignedPartitions(t);
        computeDeficits(t);
        int leftoverCount = alignDeficits(t);
        assignLeftovers(t, leftoverCount);
        realignBySwapping(t);
    }

    /**
     * Every current holder keeps its current aligned partitions up to its quota. When it has too
     * many, the released ones are those most useful to the racks having a deficit, then the ones
     * with the most replica racks since they are the easiest to place elsewhere. Misaligned
     * partitions are released so that they can be realigned, and come back to their holder if
     * that is not possible.
     */
    private void keepAlignedPartitions(int t) {
        int partitionCount = model.partitionCounts[t];
        long[] racks = model.partitionRacks[t];
        int[] demand = rackDemand(t);
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            int m = model.holderMember[i];
            Set<Integer> current = model.holderPartitions[i];
            int quota = extras.quota(m, t);
            long rackBit = 1L << model.memberRack[m];
            int aligned = 0;
            for (int p : current) {
                if (p < 0 || p >= partitionCount) {
                    continue;
                }
                previousOwner[p] = m;
                if ((racks[p] & rackBit) != 0) {
                    scratch.partitions[aligned++] = p;
                }
            }
            if (aligned > quota) {
                long[] keys = new long[aligned];
                for (int j = 0; j < aligned; j++) {
                    int p = scratch.partitions[j];
                    keys[j] = ((long) releaseUsefulness(racks[p], demand) << 32) | p;
                }
                Arrays.sort(keys);
                for (int j = 0; j < quota; j++) {
                    scratch.partitions[j] = (int) keys[j];
                }
                aligned = quota;
            }
            scratch.keep(m, current, aligned);
        }
    }

    /**
     * @return How useful a released partition with the given replica racks is to the racks
     *         having a deficit: the total deficit of those racks, then the number of racks.
     */
    private static int releaseUsefulness(long racks, int[] demand) {
        int usefulness = 0;
        long remaining = racks;
        while (remaining != 0) {
            usefulness += demand[Long.numberOfTrailingZeros(remaining)];
            remaining &= remaining - 1;
        }
        return (usefulness << 8) + Long.bitCount(racks);
    }

    /**
     * @return Per rack, the number of partitions of the topic that its members will have to
     *         receive once every holder has kept its aligned partitions up to its quota.
     */
    private int[] rackDemand(int t) {
        int partitionCount = model.partitionCounts[t];
        long[] racks = model.partitionRacks[t];
        int[] demand = new int[model.rackCount];
        // Holders: the deficit is the quota minus the aligned partitions they can keep.
        for (int i = model.holderStart[t]; i < model.holderStart[t + 1]; i++) {
            int m = model.holderMember[i];
            long rackBit = 1L << model.memberRack[m];
            int aligned = 0;
            for (int p : model.holderPartitions[i]) {
                if (p >= 0 && p < partitionCount && (racks[p] & rackBit) != 0) {
                    aligned++;
                }
            }
            scratch.deficit[scratch.participant(m)] = Math.max(0, extras.quota(m, t) - aligned);
        }
        // Receivers holding nothing need their whole quota.
        int count = receiverCount(t);
        for (int i = 0; i < count; i++) {
            int m = receiverAt(t, i);
            if (scratch.participantOf(m) == NONE) {
                demand[model.memberRack[m]] += extras.quota(m, t);
            }
        }
        IntList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            demand[model.memberRack[participants.get(i)]] += scratch.deficit[i];
            scratch.deficit[i] = 0;
        }
        return demand;
    }

    /**
     * Groups the unassigned partitions by replica rack set and the deficits by rack, then
     * computes a maximum flow to align as many of them as possible and assigns them accordingly.
     *
     * @return The number of partitions that could not be aligned, left in the partition buffer.
     */
    private int alignDeficits(int t) {
        TreeMap<Long, IntList> pool = unassignedPartitionsByRacks(t);
        if (pool.isEmpty()) {
            return 0;
        }
        IntList[] receiversByRack = new IntList[model.rackCount];
        int[] demand = rackDeficits(receiversByRack);

        // Only the first rack sets go through the flow, the partitions of the others are leftovers.
        int groupCount = Math.min(pool.size(), MAX_FLOW_GROUPS);
        long[] groupRacks = new long[groupCount];
        IntList[] groupPartitions = new IntList[groupCount];
        int[] supply = new int[groupCount];
        int leftoverCount = 0;
        int k = 0;
        for (Map.Entry<Long, IntList> entry : pool.entrySet()) {
            if (k < groupCount) {
                groupRacks[k] = entry.getKey();
                groupPartitions[k] = entry.getValue();
                supply[k] = entry.getValue().size();
                k++;
            } else {
                leftoverCount = addLeftovers(entry.getValue(), 0, leftoverCount);
            }
        }
        int[][] flow = Uniform2MaxFlow.compute(groupRacks, supply, demand);
        return assignFlow(groupPartitions, flow, receiversByRack, leftoverCount);
    }

    /**
     * @return The unassigned partitions of the topic, grouped by the racks having a replica.
     */
    private TreeMap<Long, IntList> unassignedPartitionsByRacks(int t) {
        int partitionCount = model.partitionCounts[t];
        long[] racks = model.partitionRacks[t];
        TreeMap<Long, IntList> pool = new TreeMap<>();
        for (int p = 0; p < partitionCount; p++) {
            if (scratch.owner[p] == NONE) {
                pool.computeIfAbsent(racks[p], k -> new IntList(8)).add(p);
            }
        }
        return pool;
    }

    /**
     * Collects, per rack, the participants below their quota.
     *
     * @return Per rack, the total deficit of its participants.
     */
    private int[] rackDeficits(IntList[] receiversByRack) {
        int[] demand = new int[model.rackCount];
        IntList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            int deficit = scratch.deficit[i];
            if (deficit == 0) {
                continue;
            }
            int rack = model.memberRack[participants.get(i)];
            demand[rack] += deficit;
            if (receiversByRack[rack] == null) {
                receiversByRack[rack] = new IntList(8);
            }
            receiversByRack[rack].add(i);
        }
        return demand;
    }

    /**
     * Hands the partitions of every group to the participants of every rack as the flow says,
     * in participant order. The partitions the flow could not place are leftovers.
     *
     * @return The number of leftovers in the partition buffer.
     */
    private int assignFlow(IntList[] groupPartitions, int[][] flow, IntList[] receiversByRack, int leftoverCount) {
        int[] rackCursor = new int[model.rackCount];
        IntList participants = scratch.participants;
        for (int group = 0; group < groupPartitions.length; group++) {
            IntList partitions = groupPartitions[group];
            int taken = 0;
            for (int rack = 0; rack < model.rackCount; rack++) {
                IntList receivers = receiversByRack[rack];
                for (int j = 0; j < flow[group][rack]; j++) {
                    while (scratch.deficit[receivers.get(rackCursor[rack])] == 0) {
                        rackCursor[rack]++;
                    }
                    int participant = receivers.get(rackCursor[rack]);
                    scratch.owner[partitions.get(taken++)] = participants.get(participant);
                    scratch.deficit[participant]--;
                }
            }
            leftoverCount = addLeftovers(partitions, taken, leftoverCount);
        }
        return leftoverCount;
    }

    private int addLeftovers(IntList partitions, int from, int leftoverCount) {
        for (int j = from; j < partitions.size(); j++) {
            scratch.partitions[leftoverCount++] = partitions.get(j);
        }
        return leftoverCount;
    }

    /**
     * Leftovers cannot be aligned: they go back to their previous holder if it still has a
     * deficit, then to the remaining deficits in participant order.
     */
    private void assignLeftovers(int t, int leftoverCount) {
        Arrays.sort(scratch.partitions, 0, leftoverCount);
        int remaining = 0;
        for (int j = 0; j < leftoverCount; j++) {
            int p = scratch.partitions[j];
            int m = previousOwner[p];
            int participant = m == NONE ? NONE : scratch.participantOf(m);
            if (participant != NONE && scratch.deficit[participant] > 0) {
                scratch.owner[p] = m;
                scratch.deficit[participant]--;
            } else {
                scratch.partitions[remaining++] = p;
            }
        }
        int next = 0;
        int participantCount = scratch.participants.size();
        for (int j = 0; j < remaining; j++) {
            while (next < participantCount && scratch.deficit[next] == 0) {
                next++;
            }
            if (next == participantCount) {
                throw inconsistentAssignment(t);
            }
            scratch.owner[scratch.partitions[j]] = scratch.participants.get(next);
            scratch.deficit[next]--;
        }
    }

    /**
     * Swaps every misaligned partition with a partition held by a member of a rack having a
     * replica of it, when the latter has a replica in the rack of the former's member. Partitions
     * that were not previously held by their member are preferred as partners, since swapping
     * them costs nothing more.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void realignBySwapping(int t) {
        int partitionCount = model.partitionCounts[t];
        long[] racks = model.partitionRacks[t];
        boolean anyMisaligned = false;
        for (int p = 0; p < partitionCount && !anyMisaligned; p++) {
            anyMisaligned = (racks[p] & (1L << model.memberRack[scratch.owner[p]])) == 0;
        }
        if (!anyMisaligned) {
            return;
        }
        TreeMap<Long, IntList>[] bucketsByRack = new TreeMap[model.rackCount];
        for (int p = 0; p < partitionCount; p++) {
            int rack = model.memberRack[scratch.owner[p]];
            if (bucketsByRack[rack] == null) {
                bucketsByRack[rack] = new TreeMap<>();
            }
            bucketsByRack[rack].computeIfAbsent(racks[p], key -> new IntList(8)).add(p);
        }

        for (int p = 0; p < partitionCount; p++) {
            int m = scratch.owner[p];
            int rack = model.memberRack[m];
            if ((racks[p] & (1L << rack)) != 0) {
                continue;
            }
            long partnerAndRack = findSwapPartner(p, rack, racks, bucketsByRack);
            if (partnerAndRack == NONE) {
                continue;
            }
            int partner = (int) partnerAndRack;
            int partnerRack = (int) (partnerAndRack >> 32);
            int other = scratch.owner[partner];

            scratch.owner[p] = other;
            scratch.owner[partner] = m;
            markSwapped(m);
            markSwapped(other);
            bucketsByRack[rack].get(racks[p]).removeValue(p);
            bucketsByRack[partnerRack].computeIfAbsent(racks[p], key -> new IntList(8)).add(p);
            bucketsByRack[partnerRack].get(racks[partner]).removeValue(partner);
            bucketsByRack[rack].computeIfAbsent(racks[partner], key -> new IntList(8)).add(partner);
        }
    }

    /**
     * A swapped member no longer keeps all its current partitions, so its set has to be compared
     * when emitting.
     */
    private void markSwapped(int m) {
        int participant = scratch.participantOf(m);
        if (participant != NONE) {
            scratch.keptAll[participant] = false;
        }
    }

    /**
     * @return The partner partition in the low 32 bits and the rack of its member in the high 32
     *         bits, or NONE when there is no partner.
     */
    private long findSwapPartner(int p, int rack, long[] racks, TreeMap<Long, IntList>[] bucketsByRack) {
        long rackBit = 1L << rack;
        long remaining = racks[p];
        while (remaining != 0) {
            int otherRack = Long.numberOfTrailingZeros(remaining);
            remaining &= remaining - 1;
            TreeMap<Long, IntList> buckets = bucketsByRack[otherRack];
            if (buckets == null) {
                continue;
            }
            for (Map.Entry<Long, IntList> entry : buckets.entrySet()) {
                if ((entry.getKey() & rackBit) == 0 || entry.getValue().isEmpty()) {
                    continue;
                }
                IntList candidates = entry.getValue();
                int partner = candidates.get(0);
                for (int j = 0; j < candidates.size(); j++) {
                    int candidate = candidates.get(j);
                    if (previousOwner[candidate] != scratch.owner[candidate]) {
                        partner = candidate;
                        break;
                    }
                }
                return ((long) otherRack << 32) | partner;
            }
        }
        return NONE;
    }
}
