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

import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;

import java.util.Arrays;

import static org.apache.kafka.coordinator.group.assignor.uniform2.GroupModel.NONE;

/**
 * Decides which subscribers get the extra partitions of every topic, in the three phases
 * described in {@link AssignmentBuilder}: claims, fill and even out. The allocations of
 * the members follow from the result.
 */
final class ExtraPartitionAssigner {
    private final GroupModel model;
    private final GroupModel.Owners owners;
    private final GroupModel.Cohorts cohorts;
    private final GroupModel.Racks racks;
    private final ExtraPartitions extras;
    /**
     * The loads, with the members of every cohort sorted by load. Built once the claims are
     * settled, since the claims set the initial order, and used by the two following phases.
     */
    private Loads loads;
    /**
     * Per cohort of the topic at hand, the scan position in its order, see {@link #bestReceiver}.
     */
    private final int[] cursors;
    /**
     * Per topic, the members currently owning more partitions of it than the base but not
     * getting an extra partition of it: the claimants that lost, plus the members that gave a
     * backed extra partition away. Null when there are none. They are the receivers that cost
     * no move, see {@link #backedReceiver}.
     */
    private final IntList[] backedCandidates;

    ExtraPartitionAssigner(GroupModel model) {
        this.model = model;
        this.owners = model.owners();
        this.cohorts = model.cohorts();
        this.racks = model.racks();
        extras = new ExtraPartitions(model);
        cursors = new int[model.maxCohortsPerTopic()];
        backedCandidates = new IntList[model.topicCount()];
    }

    /**
     * @return The extra partitions, every one of them having a receiver.
     */
    ExtraPartitions assign() {
        int[] load = new int[model.memberCount()];
        for (int m = 0; m < model.memberCount(); m++) {
            load[m] = cohorts.baseLoad()[cohorts.memberCohort()[m]];
        }
        claim(load);
        loads = new Loads(model, load);
        fill();
        evenOut();
        return extras;
    }

    /**
     * Phase 1: for every topic, the subscribers currently owning more partitions than the base
     * partitions claim an extra partition, as it saves them a move. When there are more claims
     * than extra partitions, the least loaded claimants win, ties going to the first member by
     * id. Afterwards, every extra partition given so far is backed.
     *
     * @param load Per member, its load, starting at the base load and updated with the claims.
     */
    private void claim(int[] load) {
        long[] claimants = new long[model.memberCount()];
        for (int t = 0; t < model.topicCount(); t++) {
            int available = model.extraPartitionCount()[t];
            if (available == 0) {
                continue;
            }
            int count = 0;
            for (int i = owners.start()[t]; i < owners.start()[t + 1]; i++) {
                if (owners.validCount()[i] > model.basePartitionCount()[t]) {
                    int m = owners.member()[i];
                    claimants[count++] = ((long) load[m] << 32) | m;
                }
            }
            if (count > available) {
                Arrays.sort(claimants, 0, count);
                for (int i = available; i < count; i++) {
                    addBackedCandidate((int) claimants[i], t);
                }
                count = available;
            }
            for (int i = 0; i < count; i++) {
                int m = (int) claimants[i];
                extras.add(m, t);
                load[m]++;
            }
        }
    }

    /**
     * Phase 2: for every topic with unclaimed extra partitions, each of them goes to the least
     * loaded subscriber not getting one yet, chosen by {@link #bestReceiver}. A topic only has
     * unclaimed extra partitions when every claimant won one, so no claimant is left out here.
     * Afterwards, every extra partition has a receiver.
     */
    private void fill() {
        for (int t = 0; t < model.topicCount(); t++) {
            int needed = model.extraPartitionCount()[t] - extras.receiverCount(t);
            if (needed == 0) {
                continue;
            }
            Arrays.fill(cursors, 0, cohortCount(t), 0);
            while (needed > 0) {
                // No subscriber owning more than the base is left without an extra partition
                // here, so there is no backed receiver to look for.
                int receiver = bestReceiver(t, false);
                if (receiver == NONE) {
                    throw new PartitionAssignorException("No member can receive an extra partition of topic "
                        + model.topicIds()[t]);
                }
                give(receiver, t);
                needed--;
            }
        }
    }

    /**
     * Phase 3: while the most loaded member having a movable extra partition is at least two
     * above the least loaded member of the group, it gives one to a subscriber of that topic
     * which has none and is at least two below it. Free extra partitions are moved in a first
     * pass which only considers them; backed ones are only moved in a second pass, when still
     * needed. Taking from the most loaded member first ensures that no member gives up an extra
     * partition it would need back later. Afterwards, no single move can bring two loads closer
     * by two, and with a single subscription all loads are within one of each other.
     *
     * <p>The work is organized in rounds. Within a round, the leading extra partitions of a
     * member found without a receiver are not looked at again: the member only gets lighter
     * during the round, so they stay without one until another member gets light enough, which
     * the next round sees. A round without any move ends the pass.
     */
    private void evenOut() {
        LongHeap givers = new LongHeap(model.memberCount());
        int[] scanStart = new int[model.memberCount()];
        for (int pass = 0; pass < 2; pass++) {
            boolean backedAllowed = pass == 1;
            boolean progress = true;
            while (progress) {
                progress = false;
                int minLoad = loads.min();
                Arrays.fill(scanStart, 0);
                givers.clear();
                for (int m = 0; m < model.memberCount(); m++) {
                    if (movableCount(m, backedAllowed) > 0) {
                        givers.push(giverKey(m));
                    }
                }
                while (!givers.isEmpty()) {
                    long key = givers.pop();
                    int giver = (int) key;
                    if (-(int) (key >> 32) != loads.load[giver]) {
                        // The load changed since the member was queued: queue it again at its
                        // place.
                        givers.push(giverKey(giver));
                        continue;
                    }
                    // Nobody lighter than this member can give anything anymore.
                    if (loads.load[giver] < minLoad + 2) {
                        break;
                    }
                    int receiverLoad = transferFrom(giver, backedAllowed, minLoad, scanStart);
                    if (receiverLoad != NONE) {
                        progress = true;
                        // Only a receiver at the minimum can raise it: the giver stays above it.
                        if (receiverLoad == minLoad) {
                            minLoad = loads.min();
                        }
                        if (movableCount(giver, backedAllowed) > 0) {
                            givers.push(giverKey(giver));
                        }
                    }
                }
            }
        }
    }

    /**
     * Moves one extra partition of the giver to the best receiver at least two below it, if any.
     * Free extra partitions are preferred, then the one whose receiver is the least loaded, then
     * the lowest topic; the receiver of a topic is chosen by {@link #bestReceiver}. The topics
     * of the giver are sorted, so the scan stops at the first one that cannot be beaten: a free
     * extra partition whose receiver is as light as the lightest member of the group, or a
     * backed one in the same situation when the giver has no free one.
     *
     * @param scanStart Per member, the number of its leading extra partitions already found
     *                  without a receiver in this round, advanced here and skipped next time.
     * @return The load of the receiver before the move, or NONE if nothing was moved.
     */
    private int transferFrom(int giver, boolean backedAllowed, int minLoad, int[] scanStart) {
        int bestTopic = NONE;
        int bestReceiver = NONE;
        boolean bestBacked = true;
        int bestLoad = Integer.MAX_VALUE;
        boolean hasFree = extras.freeCountOf(giver) > 0;
        int[] topics = extras.topicsOf(giver);
        int count = extras.countOf(giver);
        // Whether every extra partition seen so far in this call was found without a receiver.
        boolean leading = true;
        for (int i = scanStart[giver]; i < count; i++) {
            int t = topics[i];
            boolean backed = model.isBacked(giver, t);
            if (backed && !bestBacked) {
                // A free extra partition was already found, which wins.
                continue;
            }
            // A backed extra partition is never movable in the first pass.
            int receiver = backed && !backedAllowed ? NONE : eligibleReceiver(giver, t);
            if (receiver == NONE) {
                if (leading) {
                    scanStart[giver] = i + 1;
                }
                continue;
            }
            leading = false;
            if (!backed && bestBacked || loads.load[receiver] < bestLoad) {
                bestTopic = t;
                bestReceiver = receiver;
                bestBacked = backed;
                bestLoad = loads.load[receiver];
                if (bestLoad == minLoad && (!backed || !hasFree)) {
                    break;
                }
            }
        }
        if (bestTopic == NONE) {
            return NONE;
        }
        take(giver, bestTopic);
        give(bestReceiver, bestTopic);
        return bestLoad;
    }

    /**
     * @return The best receiver of an extra partition of the topic when it is at least two below
     *         the giver, NONE otherwise.
     */
    private int eligibleReceiver(int giver, int t) {
        Arrays.fill(cursors, 0, cohortCount(t), 0);
        int receiver = bestReceiver(t, true);
        return receiver != NONE && loads.load[receiver] <= loads.load[giver] - 2 ? receiver : NONE;
    }

    /**
     * Finds the best receiver of an extra partition of the topic among the subscribers not
     * getting one yet: the least loaded, then, when asked to prefer backed receivers, one
     * currently owning more partitions of the topic than the base, the first by id, since the
     * extra partition then costs no move, then, when racks are in use, the one whose rack has
     * the most spare replicas of the topic, then the one in the first cohort of the topic.
     * Within a cohort, the first member in its load order wins; that order depends only on the
     * input but is not the id order, see {@link Loads}. The cursors hold the scan
     * position in the order of each cohort of the topic. Everything before a cursor gets an
     * extra partition of the topic, which stays true when loads change since a member only ever
     * swaps places with a member at or after the cursor, so the cursors only have to be reset
     * when the topic changes.
     *
     * @param preferBacked Whether to look for a backed receiver among the least loaded ones,
     *                     which costs a scan of the backed candidates of the topic.
     * @return The receiver, or NONE if every subscriber gets an extra partition of the topic.
     */
    private int bestReceiver(int t, boolean preferBacked) {
        int best = NONE;
        int bestLoad = Integer.MAX_VALUE;
        int bestSpare = Integer.MIN_VALUE;
        int start = cohorts.topicCohortStart()[t];
        for (int i = start; i < cohorts.topicCohortStart()[t + 1]; i++) {
            int[] order = loads.order(cohorts.topicCohorts()[i]);
            int at = cursors[i - start];
            while (at < order.length && extras.has(order[at], t)) {
                at++;
            }
            cursors[i - start] = at;
            if (at == order.length) {
                continue;
            }
            int m = order[at];
            int memberLoad = loads.load[m];
            if (memberLoad > bestLoad) {
                continue;
            }
            int spare = model.usesRacks() ? rackSpare(m, t) : 0;
            if (memberLoad < bestLoad || spare > bestSpare) {
                best = m;
                bestLoad = memberLoad;
                bestSpare = spare;
            }
        }
        if (preferBacked && best != NONE) {
            int backed = backedReceiver(t, bestLoad);
            if (backed != NONE) {
                best = backed;
            }
        }
        return best;
    }

    /**
     * @return The first member by id currently owning more partitions of the topic than the
     *         base, with the given load and not getting an extra partition of the topic, or
     *         NONE. Such a member keeps a partition it already owns when it gets the extra
     *         partition, which saves a move.
     */
    private int backedReceiver(int t, int receiverLoad) {
        IntList candidates = backedCandidates[t];
        int best = NONE;
        if (candidates != null) {
            for (int i = 0; i < candidates.size(); i++) {
                int m = candidates.get(i);
                if (loads.load[m] == receiverLoad && (best == NONE || m < best)) {
                    best = m;
                }
            }
        }
        return best;
    }

    private void addBackedCandidate(int m, int t) {
        if (backedCandidates[t] == null) {
            backedCandidates[t] = new IntList(4);
        }
        backedCandidates[t].add(m);
    }

    /**
     * @return The number of partitions of the topic with a replica in the rack of the member,
     *         minus the number the members of the rack get with the current extra partitions. The
     *         higher, the more room the rack has to align one more extra partition.
     */
    private int rackSpare(int m, int t) {
        int rack = racks.memberRack()[m];
        return racks.supply()[t][rack] - cohorts.rackSubscribers()[t][rack] * model.basePartitionCount()[t] - extras.countInRack(t, rack);
    }

    private int cohortCount(int t) {
        return cohorts.topicCohortStart()[t + 1] - cohorts.topicCohortStart()[t];
    }

    private int movableCount(int m, boolean backedAllowed) {
        return backedAllowed ? extras.countOf(m) : extras.freeCountOf(m);
    }

    /**
     * Orders the givers by descending load, then ascending member.
     */
    private long giverKey(int m) {
        return ((long) -loads.load[m] << 32) | m;
    }

    private void give(int m, int t) {
        extras.add(m, t);
        loads.increment(m);
        if (backedCandidates[t] != null) {
            backedCandidates[t].removeValue(m);
        }
    }

    private void take(int m, int t) {
        extras.remove(m, t);
        loads.decrement(m);
        if (model.isBacked(m, t)) {
            addBackedCandidate(m, t);
        }
    }
}
