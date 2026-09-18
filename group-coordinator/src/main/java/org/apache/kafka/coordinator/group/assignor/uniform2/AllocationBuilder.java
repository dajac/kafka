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
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntArrayList;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongArrayHeap;

import java.util.Arrays;

import static org.apache.kafka.coordinator.group.assignor.uniform2.GroupModel.NONE;

/**
 * Builds the {@link Allocations}: decides which subscribers get the extra partitions of every
 * topic, in the three phases described in {@link AssignmentBuilder}: claims, fill and even out.
 */
final class AllocationBuilder {
    private final GroupModel model;
    private final GroupModel.Owners owners;
    private final GroupModel.Cohorts cohorts;
    private final Allocations allocations;
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
    private final IntArrayList[] backedCandidates;
    /**
     * Per topic, the receiver {@link #bestReceiver} found for the even out phase and the version
     * at which it was found, see {@link #eligibleReceiver}. The version counts the changes of
     * loads and receivers: a change of the load of a member stamps its cohort and a change of
     * the receivers of a topic stamps the topic, so a memoized receiver is known to be current
     * when its version is at least those of the topic and of its cohorts.
     */
    private final int[] memoReceiver;
    private final int[] memoVersion;
    private final int[] cohortVersion;
    private final int[] topicVersion;
    private int version;
    /**
     * Per topic, whether its best receiver is memoized: only when the topic has at least
     * {@link #MEMO_MIN_SUBSCRIBERS} subscribers, since below that the scan a memo saves is as
     * cheap as checking that the memo is current.
     */
    private final boolean[] memoized;
    private static final int MEMO_MIN_SUBSCRIBERS = 64;

    AllocationBuilder(GroupModel model) {
        this.model = model;
        this.owners = model.owners();
        this.cohorts = model.cohorts();
        allocations = new Allocations(model);
        cursors = new int[model.maxCohortsPerTopic()];
        backedCandidates = new IntArrayList[model.topicCount()];
        memoReceiver = new int[model.topicCount()];
        memoVersion = new int[model.topicCount()];
        Arrays.fill(memoVersion, -1);
        cohortVersion = new int[cohorts.count()];
        topicVersion = new int[model.topicCount()];
        memoized = new boolean[model.topicCount()];
        for (int t = 0; t < model.topicCount(); t++) {
            memoized[t] = model.subscribers()[t].length >= MEMO_MIN_SUBSCRIBERS;
        }
    }

    /**
     * @return The extra partitions, every one of them having a receiver.
     */
    Allocations build() {
        int[] load = new int[model.memberCount()];
        for (int m = 0; m < model.memberCount(); m++) {
            load[m] = cohorts.baseLoad()[cohorts.memberCohort()[m]];
        }
        claim(load);
        loads = new Loads(model, load);
        fill();
        evenOut();
        return allocations;
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
                allocations.addExtra(m, t);
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
            int needed = model.extraPartitionCount()[t] - allocations.extraReceiverCount(t);
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
        LongArrayHeap givers = new LongArrayHeap(model.memberCount());
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
        boolean hasFree = allocations.freeExtraCount(giver) > 0;
        int[] topics = allocations.topicsOf(giver);
        int count = allocations.extraCount(giver);
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
     * The best receiver of a topic only depends on the loads of the members of its cohorts and on
     * its receivers, so it is memoized until one of them changes: the even out phase checks the
     * topics of many givers between two moves, and every check would otherwise scan the members
     * of the topic already getting an extra partition of it.
     *
     * @return The best receiver of an extra partition of the topic when it is at least two below
     *         the giver, NONE otherwise.
     */
    private int eligibleReceiver(int giver, int t) {
        int receiver;
        if (!memoized[t]) {
            Arrays.fill(cursors, 0, cohortCount(t), 0);
            receiver = bestReceiver(t, true);
        } else if (memoCurrent(t)) {
            receiver = memoReceiver[t];
        } else {
            Arrays.fill(cursors, 0, cohortCount(t), 0);
            receiver = bestReceiver(t, true);
            memoReceiver[t] = receiver;
            memoVersion[t] = version;
        }
        return receiver != NONE && loads.load[receiver] <= loads.load[giver] - 2 ? receiver : NONE;
    }

    /**
     * @return Whether the memoized receiver of the topic is current: nothing it depends on
     *         changed since it was found.
     */
    private boolean memoCurrent(int t) {
        int v = memoVersion[t];
        if (v < topicVersion[t]) {
            return false;
        }
        for (int i = cohorts.topicCohortStart()[t]; i < cohorts.topicCohortStart()[t + 1]; i++) {
            if (v < cohortVersion[cohorts.topicCohorts()[i]]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the best receiver of an extra partition of the topic among the subscribers not
     * getting one yet: the least loaded, then, when asked to prefer backed receivers, one
     * currently owning more partitions of the topic than the base, the first by id, since the
     * extra partition then costs no move, then the one in the first cohort of the topic.
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
        int start = cohorts.topicCohortStart()[t];
        for (int i = start; i < cohorts.topicCohortStart()[t + 1]; i++) {
            int[] order = loads.order(cohorts.topicCohorts()[i]);
            int at = cursors[i - start];
            while (at < order.length && allocations.hasExtra(order[at], t)) {
                at++;
            }
            cursors[i - start] = at;
            if (at == order.length) {
                continue;
            }
            int m = order[at];
            if (loads.load[m] < bestLoad) {
                best = m;
                bestLoad = loads.load[m];
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
        IntArrayList candidates = backedCandidates[t];
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
            backedCandidates[t] = new IntArrayList(4);
        }
        backedCandidates[t].add(m);
    }

    private int cohortCount(int t) {
        return cohorts.topicCohortStart()[t + 1] - cohorts.topicCohortStart()[t];
    }

    private int movableCount(int m, boolean backedAllowed) {
        return backedAllowed ? allocations.extraCount(m) : allocations.freeExtraCount(m);
    }

    /**
     * Orders the givers by descending load, then ascending member.
     */
    private long giverKey(int m) {
        return ((long) -loads.load[m] << 32) | m;
    }

    private void give(int m, int t) {
        allocations.addExtra(m, t);
        loads.increment(m);
        if (backedCandidates[t] != null) {
            backedCandidates[t].remove(m);
        }
        stamp(m, t);
    }

    private void take(int m, int t) {
        allocations.removeExtra(m, t);
        loads.decrement(m);
        if (model.isBacked(m, t)) {
            addBackedCandidate(m, t);
        }
        stamp(m, t);
    }

    /**
     * Records that the load of the member and the receivers of the topic changed, see
     * {@link #memoCurrent}.
     */
    private void stamp(int m, int t) {
        version++;
        cohortVersion[cohorts.memberCohort()[m]] = version;
        topicVersion[t] = version;
    }
}
