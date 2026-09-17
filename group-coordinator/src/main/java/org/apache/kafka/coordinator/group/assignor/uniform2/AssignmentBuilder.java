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
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.Uniform2Assignor;

import java.util.Map;

/**
 * Builds the assignment of the {@link Uniform2Assignor}.
 *
 * <p><b>Goal.</b> Given the members of a group, their subscriptions, their current partitions
 * and the partitions of the subscribed topics, build an assignment with these properties:
 * <ul>
 *     <li><b>Complete:</b> every partition of every subscribed topic is assigned to exactly one
 *     member subscribed to the topic.</li>
 *     <li><b>Spread:</b> the partitions of every topic are split evenly among its subscribers.
 *     For a topic with {@code p} partitions and {@code n} subscribers, every subscriber gets
 *     either {@code p / n} or {@code p / n + 1} of them.</li>
 *     <li><b>Balanced:</b> when all members have the same subscription, the numbers of
 *     partitions per member differ by at most one. When subscriptions differ, the assignment
 *     is as balanced as the subscriptions allow: no partition can be moved between two
 *     subscribers of its topic so that their totals get closer by two, without breaking the
 *     spread.</li>
 *     <li><b>Sticky:</b> partitions only move when the properties above require it. When a
 *     member leaves or joins, essentially only the partitions it owned or is owed move, plus
 *     the few that keep the other members balanced when the change of allocations leaves them
 *     uneven. An assignment that already has the properties is returned as is, down to the
 *     same partition set instances, so unchanged members are cheap to recognize. The balance
 *     reached is one of several possible, and another one could occasionally need one move
 *     less.</li>
 *     <li><b>Deterministic:</b> the result only depends on the content of the input, never on
 *     the order in which members or topics are iterated.</li>
 *     <li><b>Rack aware, on demand:</b> when enabled, members receive partitions having a
 *     replica in their rack whenever this does not conflict with the properties above. When
 *     disabled, none of the rack awareness code runs.</li>
 * </ul>
 *
 * <p><b>Vocabulary.</b>
 * <ul>
 *     <li>A <i>subscriber</i> of a topic is a member whose subscription contains the topic.</li>
 *     <li>Every subscriber of a topic gets {@code p / n} partitions of it, its <i>base
 *     partitions</i>. The {@code p % n} remaining partitions are the <i>extra partitions</i>
 *     of the topic. Each extra partition goes to a distinct subscriber, which gets one
 *     partition more than the others. The first phases decide which subscribers get the extra
 *     partitions, not which partition ids they are: the ids are only chosen in the last phase,
 *     together with the ids of the base partitions.</li>
 *     <li>The <i>allocation</i> of a subscriber for a topic is its number of base partitions, plus
 *     one if it gets an extra partition.</li>
 *     <li>The <i>load</i> of a member is the sum of its allocations, that is the number of
 *     partitions it will be assigned. It is its <i>base load</i>, the base partitions of all its
 *     topics added up, plus the number of extra partitions it gets.</li>
 *     <li>A <i>cohort</i> is a set of members with the same subscription, and the same rack
 *     when rack awareness is in use. The members of a cohort have the same base load and are
 *     eligible for the same extra partitions. A group where all members have the same
 *     subscription has a single cohort, or one per rack.</li>
 *     <li>The <i>current</i> partitions of a member are the ones it owns in the input, and the
 *     member is their <i>owner</i>. A current partition is <i>stale</i> when its topic no longer
 *     exists or is no longer subscribed by its owner, or when its id is beyond the partition
 *     count of the topic; stale partitions are ignored.</li>
 *     <li>An extra partition is <i>backed</i> when the member getting it currently owns more
 *     partitions of the topic than the base: it lets the member keep one of them. Otherwise
 *     the extra partition is <i>free</i> and giving it to another member costs nothing.</li>
 *     <li>A partition is <i>aligned</i> with a member when one of its replicas is in the rack
 *     of the member. The <i>supply</i> of a rack for a topic is the number of its partitions
 *     having a replica in the rack.</li>
 *     <li>In the partition phase, the <i>participants</i> of a topic are its subscribers owning
 *     or receiving partitions of it. A participant below its allocation is a <i>receiver</i>,
 *     and the difference is its <i>deficit</i>. The partitions an owner gives up above its
 *     allocation are <i>released</i>. A released partition that the rack aware flow does not
 *     place is a <i>leftover</i>, which returns to its owner. In the even out phase, the
 *     <i>receiver</i> of an extra partition is the member it moves to.</li>
 * </ul>
 *
 * <p><b>Key idea.</b> The algorithm decides allocations before partition ids. The spread property
 * holds by construction of the allocations, and the balance of the group only depends on which
 * subscribers get the extra partitions. So the problem reduces to distributing the extra
 * partitions, which are few, and partition ids are only handled once the allocations are final,
 * one topic at a time. Stickiness comes from letting members keep the extra partitions and
 * the partition ids they currently have whenever the allocations allow it.
 *
 * <p><b>Phase 1, claims.</b> For every topic, the subscribers currently owning more
 * partitions than the base claim an extra partition, as it saves them a move. When there are
 * more claims than extra partitions, the least loaded claimants win, ties going to the first
 * member by id. Loads count the extra partitions claimed so far, topics being processed in id
 * order. Afterwards, every claimed extra partition is backed.
 *
 * <p><b>Phase 2, fill.</b> For every topic with unclaimed extra partitions, each of them goes
 * to the least loaded subscriber not getting one yet. When rack aware, ties go to the member
 * whose rack has the most spare replicas of the topic, so that its extra partition can be
 * aligned. The remaining ties go to the first cohort of the topic, and within it to the first
 * member in its load order, described below. Afterwards, every extra partition has a member
 * and the spread and completeness properties are settled.
 *
 * <p><b>Phase 3, even out.</b> Claims are local to a topic, so a member that kept many of them
 * may be well above the others. While the most loaded member having a movable extra
 * partition is at least two above the least loaded member of the group, it gives one of them
 * to a subscriber of that topic which has none and is at least two below it, the least loaded
 * such subscriber. Moving an extra partition across a gap of one would only swap who is
 * heavier, so the gap must be two: each move strictly reduces the imbalance, which guarantees
 * termination. Taking extra partitions from the most loaded member first ensures that no
 * member gives up one it would need back later. Free extra partitions are moved first, in a
 * first pass which only considers them, and backed ones are only moved in a second pass when
 * still needed, since each of them costs a partition move. Among the extra partitions of a
 * member, a free one wins over a backed one, then the one going to the least loaded receiver,
 * then the lowest topic. The receiver of a topic is chosen as in the fill phase, except that
 * among the least loaded candidates one currently owning more partitions of the topic than
 * the base wins, the first by id, since the extra partition then costs no move. When all
 * members have the same subscription, every member is eligible for every extra partition and
 * the phase ends with all loads within one of each other. When subscriptions differ, it ends
 * when no single move can help; balancing further would need chains of moves through several
 * cohorts, which are not attempted.
 *
 * <p><b>Phase 4, partitions.</b> Every topic is handled on its own, with the allocations now final.
 * Each current owner keeps its current partitions up to its allocation, the lowest partition ids
 * first, and releases the rest. The partitions that nobody kept are then handed out, in
 * ascending order, to the members below their allocation: the current owners of the topic first,
 * then its other subscribers, each in member id order. When a
 * topic has fewer partitions than subscribers, its base is zero and only the members getting
 * an extra partition receive one. A topic where every owner already has exactly its allocation,
 * and the owners together have every partition, is emitted as is. A member whose partitions
 * of a topic did not change gets its current set back, not a copy.
 *
 * <p><b>Example.</b> Members A, B and C subscribe to topic T1 with 5 partitions and topic T2
 * with 4 partitions, and own nothing yet.
 * <pre>
 * T1: 5 / 3 = 1 base partition each, two extra partitions.
 * T2: 4 / 3 = 1 base partition each, one extra partition.
 * Base load 2 for everyone, load order A, B, C.
 * Claims:   nothing is owned, so no claim.
 * Fill:     the first extra partition of T1 goes to A (load 3), which moves to the end of the
 *           load order, now C, B, A. The second one goes to C (load 3), and the order becomes
 *           B, C, A. The extra partition of T2 goes to B (load 3).
 * Even out: all loads are 3, nothing to do.
 * Partitions:
 *   T1, allocations A 2, B 1, C 2:  A gets 0 and 1, B gets 2, C gets 3 and 4.
 *   T2, allocations A 1, B 2, C 1:  A gets 0, B gets 1 and 2, C gets 3.
 * </pre>
 * Member D then joins with the same subscription.
 * <pre>
 * T1: 5 / 4 = 1 base partition each, one extra partition.
 * T2: 4 / 4 = 1 base partition each, no extra partition.
 * Base load 2 for everyone.
 * Claims:   A and C both own 2 partitions of T1, more than the base: two claims for one extra
 *           partition. Both are at load 2, A wins by id. Loads: A 3, B 2, C 2, D 2.
 * Fill:     every extra partition has a member.
 * Even out: A is at 3 and the minimum is 2, a gap of one: nothing to do.
 * Partitions:
 *   T1, allocations A 2, B 1, C 1, D 1:  A keeps 0 and 1, B keeps 2, C keeps 3 and releases 4,
 *                                   D receives 4.
 *   T2, allocations 1 for everyone:      A keeps 0, B keeps 1 and releases 2, C keeps 3,
 *                                   D receives 2.
 * Two partitions moved, exactly the two that D is owed. Loads: A 3, B 2, C 2, D 2.
 * </pre>
 * A second example shows the claims and the even out phase working together. Members A and B
 * subscribe to topics T1 and T2 with 3 partitions each, and A currently owns all six.
 * <pre>
 * T1 and T2: 3 / 2 = 1 base partition each, one extra partition each. Base load 2 for both.
 * Claims:   A owns 3 partitions of T1, more than the base: it claims the extra partition
 *           (load 3). The same happens for T2 (load 4). B stays at 2.
 * Fill:     every extra partition has a member.
 * Even out: A is at 4 and B at 2, a gap of two, so A must give one. Both of its extra
 *           partitions are backed, so the first pass does nothing and the second pass moves
 *           the one of the lowest topic, T1, to B. Loads: A 3, B 3. Done.
 * Partitions:
 *   T1, allocations A 1, B 2:  A keeps 0 and releases 1 and 2, which B receives.
 *   T2, allocations A 2, B 1:  A keeps 0 and 1 and releases 2, which B receives.
 * Three partitions moved, exactly the three that B is owed.
 * </pre>
 *
 * <p><b>Rack awareness.</b> It is used when enabled, every member has a rack and there are
 * between two and 64 distinct racks; otherwise the result is exactly the one of the plain
 * algorithm. It never changes the allocations, so the properties above are kept as they are. It
 * acts in two places: as the tie breaker described in the fill and even out phases, and in
 * the partition phase, which for each topic becomes:
 * <ol>
 *     <li><b>Keep:</b> each current owner keeps its current aligned partitions up to its
 *     allocation. Misaligned partitions are released so that they can be realigned, and come back
 *     to their owner later if they cannot be. An owner with more aligned partitions than its
 *     allocation releases first the ones most useful to the racks whose members are below their
 *     allocations, then the ones with the most replica racks, which are the easiest to place.</li>
 *     <li><b>Align:</b> the released partitions are grouped by the set of racks having their
 *     replicas, and the members below their allocation are grouped by rack. The largest number of
 *     partitions that can be handed to a member in one of their replica racks is found with
 *     a maximum flow from the partition groups, through the racks, to the demand of each
 *     rack. A greedy match can get stuck, for instance by giving a partition with replicas in
 *     racks 1 and 2 to rack 1 when a partition with replicas in racks 1 and 3 was the only
 *     one able to serve rack 1. The flow network has one node per distinct replica rack set
 *     among the released partitions and one per rack: a handful with the usual three racks.
 *     Within a rack, the partitions go to the members below their allocation, the current owners
 *     of the topic first, then its other subscribers, each in member id order. Within a group,
 *     the partitions handed out are first those that have to move anyway: the ones nobody
 *     owns, and the ones whose previous owner has no deficit left once the flow is served, or
 *     already has as many of them set aside as its deficit. The partitions whose owner can
 *     take them back are only handed out when the flow needs more of the group, so that they
 *     are the ones left over.</li>
 *     <li><b>Leftovers:</b> the partitions that cannot be aligned go back to their previous
 *     owner if it is still below its allocation, then to the remaining members below their
 *     allocation in that same order. Thanks to the order of the align step, a leftover goes
 *     back to its owner whenever its group has enough other partitions to hand out.</li>
 *     <li><b>Swap:</b> for each partition still misaligned, look for a partition owned by a
 *     member in one of its replica racks which has itself a replica in the rack of the
 *     misaligned owner, and swap the two. Both members keep their allocations. Partitions that
 *     were not previously owned by their member are preferred as partners, as swapping them
 *     costs nothing more. This repairs the misalignments left by the greedy keep step.</li>
 * </ol>
 * A settled topic whose partitions are all aligned is emitted as is. With two replicas per
 * partition spread over three racks and a similar number of members per rack, every partition
 * ends up aligned in practice.
 *
 * <p><b>Determinism and stability.</b> Members and topics are sorted by id, and the members of
 * a cohort are kept in a load order that only depends on the input: it starts in id order and
 * every change of a load moves one member within it in a fixed way. Ties between equally
 * loaded members are broken by that order, so they are deterministic but not always in id
 * order. The result thus does not depend on iteration orders. An assignment having all
 * the properties is a fixed point: every member keeps its extra partitions and its partition
 * ids, and the output holds the very partition set instances of the input.
 *
 * <p><b>Input assumptions.</b> The current assignment is consistent, every partition being
 * owned by at most one member, as guaranteed by the target assignment maintained by the
 * coordinator. Current partitions of topics that are no longer subscribed or no longer exist,
 * and partition ids beyond the current partition count of a topic, are stale.
 *
 * <p><b>Cost.</b> Members are kept sorted by load within their cohort, with constant time
 * updates when a load changes by one, so the phases distributing the extra partitions are
 * linear in their number times the number of cohorts subscribed to the topic, plus, for the
 * extra partitions that the even out phase moves, the members that claimed one of the same
 * topic without getting it. The partition
 * phase is linear in the total number of partitions. Rack awareness adds, per topic with
 * partitions to realign, a maximum flow over a network with one node per distinct replica
 * rack set among the released partitions, and a scan of the partitions for the swaps.
 *
 * <p><b>Structure.</b> The code follows the phases. {@link GroupModel} normalizes the
 * input: members and topics numbered, subscribers, current partitions, base and extra partition
 * counts, cohorts and racks. {@link ExtraPartitionAssigner} runs the claims, fill and
 * even out phases on {@link ExtraPartitions}, tracking loads with {@link Loads}.
 * {@link PartitionAssigner} runs the partition phase, with
 * {@link RackAwarePartitionAssigner} taking over when racks are in use, and
 * {@link AssignmentResult} builds the group assignment.
 */
public final class AssignmentBuilder {
    private final GroupSpec groupSpec;
    private final SubscribedTopicDescriber subscribedTopicDescriber;
    private final boolean rackAwareEnabled;

    public AssignmentBuilder(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber,
        boolean rackAwareEnabled
    ) {
        this.groupSpec = groupSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.rackAwareEnabled = rackAwareEnabled;
    }

    public GroupAssignment build() {
        GroupModel model = new GroupModel(groupSpec, subscribedTopicDescriber, rackAwareEnabled);
        if (model.topicCount() == 0) {
            return new GroupAssignment(Map.of());
        }
        ExtraPartitions extras = new ExtraPartitionAssigner(model).assign();
        PartitionAssigner partitionAssigner = model.usesRacks()
            ? new RackAwarePartitionAssigner(model, extras)
            : new PartitionAssigner(model, extras);
        return partitionAssigner.assign();
    }
}
