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

import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntArrayList;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.GroupModel.NONE;

/**
 * The partition phase when racks are in use, see {@link AssignmentBuilder}. The allocations
 * are the same as without racks; only the choice of partition ids changes, so that as many
 * members as possible get partitions having a replica in their rack. For each topic:
 * <ol>
 *     <li><b>Keep:</b> each current owner keeps its current aligned partitions up to its
 *     allocation. Misaligned partitions are released so that they can be realigned. An owner
 *     with more aligned partitions than its allocation releases first the ones most useful to
 *     the racks whose members are below their allocations, then the ones with the most replica
 *     racks.</li>
 *     <li><b>Align:</b> the released partitions are handed to the members below their allocation
 *     with a maximum flow from replica rack sets to racks, see {@link MaxFlow}. Among
 *     the partitions sharing the same replica racks, the ones whose previous owner can take
 *     them back are handed out last, so that they are the ones left over.</li>
 *     <li><b>Leftovers:</b> the partitions that cannot be aligned go back to their previous
 *     owner if it is still below its allocation, then to the remaining members below their
 *     allocation, the current owners of the topic first, then its other subscribers, each in
 *     member id order.</li>
 *     <li><b>Swap:</b> each partition still misaligned is swapped with a partition owned by a
 *     member in one of its replica racks, when that partition has a replica in the rack of the
 *     misaligned owner.</li>
 * </ol>
 * A settled topic is only emitted as is when all its partitions are aligned, since it may
 * otherwise be realigned by swaps.
 */
final class RackAwarePartitionAssigner extends PartitionAssigner {
    /**
     * The flow network takes one node per distinct replica rack set among the released
     * partitions. Beyond this many, the remaining partitions are treated as leftovers, which
     * bounds the size of the network with many racks and replicas.
     */
    private static final int MAX_FLOW_GROUPS = 1024;

    /**
     * Per partition of the topic at hand, the member currently owning it, or NONE.
     */
    private final int[] previousOwner;
    /**
     * Per unit of the flow, in group then rack order, the participant receiving it.
     */
    private final int[] flowReceivers;
    /**
     * Per participant, while the flow is handed out, the number of leftovers it can still take
     * back.
     */
    private final int[] returnable;
    private final GroupModel.Racks racks;

    RackAwarePartitionAssigner(GroupModel model, Allocations allocations) {
        super(model, allocations);
        this.racks = model.racks();
        previousOwner = new int[model.maxPartitionsPerTopic()];
        flowReceivers = new int[model.maxPartitionsPerTopic()];
        returnable = new int[model.memberCount()];
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
        long[] replicaRacks = racks.partitionRacks()[t];
        for (int i = owners.start()[t]; i < owners.start()[t + 1]; i++) {
            long rackBit = 1L << racks.memberRack()[owners.member()[i]];
            for (int p : owners.partitions()[i]) {
                if ((replicaRacks[p] & rackBit) == 0) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    void assignTopic(int t) {
        Arrays.fill(previousOwner, 0, model.partitionCounts()[t], NONE);
        keepAlignedPartitions(t);
        computeDeficits(t);
        int leftoverCount = alignDeficits(t);
        assignLeftovers(t, leftoverCount);
        realignBySwapping(t);
    }

    /**
     * Every current owner keeps its current aligned partitions up to its allocation. When it
     * has too many, the released ones are those most useful to the racks having a deficit,
     * then the ones with the most replica racks since they are the easiest to place elsewhere.
     * Misaligned partitions are released so that they can be realigned, and come back to their
     * owner if that is not possible.
     */
    private void keepAlignedPartitions(int t) {
        int partitionCount = model.partitionCounts()[t];
        long[] replicaRacks = racks.partitionRacks()[t];
        int[] demand = rackDemand(t);
        for (int i = owners.start()[t]; i < owners.start()[t + 1]; i++) {
            int m = owners.member()[i];
            Set<Integer> current = owners.partitions()[i];
            int allocation = allocations.allocation(m, t);
            long rackBit = 1L << racks.memberRack()[m];
            int aligned = 0;
            for (int p : current) {
                if (p < 0 || p >= partitionCount) {
                    continue;
                }
                previousOwner[p] = m;
                if ((replicaRacks[p] & rackBit) != 0) {
                    scratch.partitions[aligned++] = p;
                }
            }
            if (aligned > allocation) {
                long[] keys = new long[aligned];
                for (int j = 0; j < aligned; j++) {
                    int p = scratch.partitions[j];
                    keys[j] = ((long) releaseUsefulness(replicaRacks[p], demand) << 32) | p;
                }
                Arrays.sort(keys);
                for (int j = 0; j < allocation; j++) {
                    scratch.partitions[j] = (int) keys[j];
                }
                aligned = allocation;
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
     *         receive once every owner has kept its aligned partitions up to its allocation.
     */
    private int[] rackDemand(int t) {
        int partitionCount = model.partitionCounts()[t];
        long[] replicaRacks = racks.partitionRacks()[t];
        int[] demand = new int[racks.count()];
        // Owners: the deficit is the allocation minus the aligned partitions they can keep.
        for (int i = owners.start()[t]; i < owners.start()[t + 1]; i++) {
            int m = owners.member()[i];
            long rackBit = 1L << racks.memberRack()[m];
            int aligned = 0;
            for (int p : owners.partitions()[i]) {
                if (p >= 0 && p < partitionCount && (replicaRacks[p] & rackBit) != 0) {
                    aligned++;
                }
            }
            scratch.deficit[scratch.participant(m)] = Math.max(0, allocations.allocation(m, t) - aligned);
        }
        // Receivers owning nothing need their whole allocation.
        int count = receiverCount(t);
        for (int i = 0; i < count; i++) {
            int m = receiverAt(t, i);
            if (scratch.participantOf(m) == NONE) {
                demand[racks.memberRack()[m]] += allocations.allocation(m, t);
            }
        }
        IntArrayList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            demand[racks.memberRack()[participants.get(i)]] += scratch.deficit[i];
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
        TreeMap<Long, IntArrayList> pool = unassignedPartitionsByRacks(t);
        if (pool.isEmpty()) {
            return 0;
        }
        IntArrayList[] receiversByRack = new IntArrayList[racks.count()];
        int[] demand = rackDeficits(receiversByRack);

        // Only the first rack sets go through the flow, the partitions of the others are leftovers.
        int groupCount = Math.min(pool.size(), MAX_FLOW_GROUPS);
        long[] groupRacks = new long[groupCount];
        IntArrayList[] groupPartitions = new IntArrayList[groupCount];
        int[] supply = new int[groupCount];
        int leftoverCount = 0;
        int k = 0;
        for (Map.Entry<Long, IntArrayList> entry : pool.entrySet()) {
            if (k < groupCount) {
                groupRacks[k] = entry.getKey();
                groupPartitions[k] = entry.getValue();
                supply[k] = entry.getValue().size();
                k++;
            } else {
                leftoverCount = addLeftovers(entry.getValue(), 0, leftoverCount);
            }
        }
        int[][] flow = MaxFlow.compute(groupRacks, supply, demand);
        return assignFlow(groupPartitions, flow, receiversByRack, leftoverCount);
    }

    /**
     * @return The unassigned partitions of the topic, grouped by the racks having a replica.
     */
    private TreeMap<Long, IntArrayList> unassignedPartitionsByRacks(int t) {
        int partitionCount = model.partitionCounts()[t];
        long[] replicaRacks = racks.partitionRacks()[t];
        TreeMap<Long, IntArrayList> pool = new TreeMap<>();
        for (int p = 0; p < partitionCount; p++) {
            if (scratch.owner[p] == NONE) {
                pool.computeIfAbsent(replicaRacks[p], k -> new IntArrayList(8)).add(p);
            }
        }
        return pool;
    }

    /**
     * Collects, per rack, the participants below their allocation.
     *
     * @return Per rack, the total deficit of its participants.
     */
    private int[] rackDeficits(IntArrayList[] receiversByRack) {
        int[] demand = new int[racks.count()];
        IntArrayList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            int deficit = scratch.deficit[i];
            if (deficit == 0) {
                continue;
            }
            int rack = racks.memberRack()[participants.get(i)];
            demand[rack] += deficit;
            if (receiversByRack[rack] == null) {
                receiversByRack[rack] = new IntArrayList(8);
            }
            receiversByRack[rack].add(i);
        }
        return demand;
    }

    /**
     * Hands the partitions of every group to the participants of every rack as the flow says.
     * The partitions the flow does not place are leftovers, which cost no move when they go back
     * to their previous owner, so the choice of the partitions handed out within a group
     * matters: the receivers are chosen first, since they do not depend on it, which leaves in
     * the deficits what every participant can still take back once the flow is served. The
     * partitions of a group are then handed out in this order: first those that have to move
     * anyway, because nobody owns them, or their owner has no deficit left, or their owner
     * already has as many partitions set aside as its deficit, and only when the flow needs more
     * the ones set aside, both in ascending order. Whatever is not handed out is left over. The
     * partitions set aside go back to their owner in {@link #assignLeftovers}; the owner of a
     * partition handed out in the second pass gets its room for one more leftover back.
     *
     * <p>No partition goes through the flow to its own previous owner, so setting one aside
     * never costs an aligned partition: an owner releases a partition either because it is
     * misaligned, so the owner is not in one of the racks of the group, or because it has more
     * aligned partitions than its allocation, so the owner has no deficit at all.
     *
     * @return The number of leftovers in the partition buffer.
     */
    private int assignFlow(IntArrayList[] groupPartitions, int[][] flow, IntArrayList[] receiversByRack, int leftoverCount) {
        chooseReceivers(flow, receiversByRack);
        IntArrayList participants = scratch.participants;
        for (int i = 0; i < participants.size(); i++) {
            returnable[i] = scratch.deficit[i];
        }
        int next = 0;
        for (int group = 0; group < groupPartitions.length; group++) {
            IntArrayList partitions = groupPartitions[group];
            int end = next + sum(flow[group]);
            for (int j = 0; j < partitions.size(); j++) {
                int p = partitions.get(j);
                int owner = ownerParticipant(p);
                if (owner != NONE && returnable[owner] > 0) {
                    returnable[owner]--;
                } else if (next < end) {
                    scratch.owner[p] = participants.get(flowReceivers[next++]);
                }
            }
            for (int j = 0; j < partitions.size(); j++) {
                int p = partitions.get(j);
                if (scratch.owner[p] != NONE) {
                    continue;
                }
                if (next < end) {
                    returnable[ownerParticipant(p)]++;
                    scratch.owner[p] = participants.get(flowReceivers[next++]);
                } else {
                    scratch.partitions[leftoverCount++] = p;
                }
            }
        }
        return leftoverCount;
    }

    /**
     * Chooses the receivers of every unit of the flow, per group then per rack: the participants
     * of the rack below their allocation, in participant order, whose deficits are lowered as
     * they go.
     */
    private void chooseReceivers(int[][] flow, IntArrayList[] receiversByRack) {
        int[] rackCursor = new int[racks.count()];
        int next = 0;
        for (int[] groupFlow : flow) {
            for (int rack = 0; rack < racks.count(); rack++) {
                IntArrayList receivers = receiversByRack[rack];
                for (int j = 0; j < groupFlow[rack]; j++) {
                    while (scratch.deficit[receivers.get(rackCursor[rack])] == 0) {
                        rackCursor[rack]++;
                    }
                    int participant = receivers.get(rackCursor[rack]);
                    flowReceivers[next++] = participant;
                    scratch.deficit[participant]--;
                }
            }
        }
    }

    private static int sum(int[] values) {
        int total = 0;
        for (int value : values) {
            total += value;
        }
        return total;
    }

    /**
     * @return The participant currently owning the partition, or NONE.
     */
    private int ownerParticipant(int p) {
        int m = previousOwner[p];
        return m == NONE ? NONE : scratch.participantOf(m);
    }

    private int addLeftovers(IntArrayList partitions, int from, int leftoverCount) {
        for (int j = from; j < partitions.size(); j++) {
            scratch.partitions[leftoverCount++] = partitions.get(j);
        }
        return leftoverCount;
    }

    /**
     * Leftovers cannot be aligned: they go back to their previous owner if it still has a
     * deficit, then to the remaining deficits in participant order. The flow leaves over, as far
     * as it can, partitions whose owner can take them back, so most leftovers cost no move.
     */
    private void assignLeftovers(int t, int leftoverCount) {
        Arrays.sort(scratch.partitions, 0, leftoverCount);
        int remaining = 0;
        for (int j = 0; j < leftoverCount; j++) {
            int p = scratch.partitions[j];
            int participant = ownerParticipant(p);
            if (participant != NONE && scratch.deficit[participant] > 0) {
                scratch.owner[p] = previousOwner[p];
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
     * Swaps every misaligned partition with a partition owned by a member of a rack having a
     * replica of it, when the latter has a replica in the rack of the former's member. Partitions
     * that were not previously owned by their member are preferred as partners, since swapping
     * them costs nothing more.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void realignBySwapping(int t) {
        int partitionCount = model.partitionCounts()[t];
        long[] replicaRacks = racks.partitionRacks()[t];
        boolean anyMisaligned = false;
        for (int p = 0; p < partitionCount && !anyMisaligned; p++) {
            anyMisaligned = (replicaRacks[p] & (1L << racks.memberRack()[scratch.owner[p]])) == 0;
        }
        if (!anyMisaligned) {
            return;
        }
        TreeMap<Long, IntArrayList>[] bucketsByRack = new TreeMap[racks.count()];
        for (int p = 0; p < partitionCount; p++) {
            int rack = racks.memberRack()[scratch.owner[p]];
            if (bucketsByRack[rack] == null) {
                bucketsByRack[rack] = new TreeMap<>();
            }
            bucketsByRack[rack].computeIfAbsent(replicaRacks[p], key -> new IntArrayList(8)).add(p);
        }

        for (int p = 0; p < partitionCount; p++) {
            int m = scratch.owner[p];
            int rack = racks.memberRack()[m];
            if ((replicaRacks[p] & (1L << rack)) != 0) {
                continue;
            }
            long partnerAndRack = findSwapPartner(p, rack, replicaRacks, bucketsByRack);
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
            bucketsByRack[rack].get(replicaRacks[p]).remove(p);
            bucketsByRack[partnerRack].computeIfAbsent(replicaRacks[p], key -> new IntArrayList(8)).add(p);
            bucketsByRack[partnerRack].get(replicaRacks[partner]).remove(partner);
            bucketsByRack[rack].computeIfAbsent(replicaRacks[partner], key -> new IntArrayList(8)).add(partner);
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
    private long findSwapPartner(int p, int rack, long[] replicaRacks, TreeMap<Long, IntArrayList>[] bucketsByRack) {
        long rackBit = 1L << rack;
        long remaining = replicaRacks[p];
        while (remaining != 0) {
            int otherRack = Long.numberOfTrailingZeros(remaining);
            remaining &= remaining - 1;
            TreeMap<Long, IntArrayList> buckets = bucketsByRack[otherRack];
            if (buckets == null) {
                continue;
            }
            for (Map.Entry<Long, IntArrayList> entry : buckets.entrySet()) {
                if ((entry.getKey() & rackBit) == 0 || entry.getValue().isEmpty()) {
                    continue;
                }
                IntArrayList candidates = entry.getValue();
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
