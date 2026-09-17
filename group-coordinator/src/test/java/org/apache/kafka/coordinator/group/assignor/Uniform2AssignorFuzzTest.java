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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Event;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Kind;
import org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.alignedPartitions;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.load;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.revocations;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.subscriptionType;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.withAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fuzzer of the uniform2 assignor. Random groups and clusters, see {@link Uniform2FuzzScenario},
 * go through random sequences of events, and every resulting assignment is checked for the
 * properties documented in {@link AssignmentBuilder}:
 * <ul>
 *     <li>complete, spread and balanced, with {@link AssignmentTestUtils#assertValidAssignment};</li>
 *     <li>deterministic: the members, the topics of their subscriptions and the entries of their
 *     current assignments given in reverse order produce the same assignment;</li>
 *     <li>a fixed point: feeding the assignment back returns the very same assignment maps, hence
 *     the very same partition set instances;</li>
 *     <li>sticky: with a single subscription and no rack awareness, the partitions moved among
 *     the members present both before and after a single leave or join, that is every moved
 *     partition for a leave and every one beyond the joiner's intake for a join, are at most the
 *     extra partitions of the group, the sum over the topics of the partition count modulo the
 *     subscriber count: only backed extra partitions cost a move in the even out phase and each
 *     moves at most once. For groups of up to {@link #ORACLE_MAX_MEMBERS} members and
 *     {@link #ORACLE_MAX_TOPICS} topics, the number of moved partitions is at most one above the
 *     smallest of all the assignments having the properties, found by brute force; the fill and
 *     even out phases settle ties without looking ahead and occasionally reach a balance costing
 *     one move more than another one, with a single subscription as well as with several, and
 *     the summary line counts these cases per subscription kind;</li>
 *     <li>sticky with racks: for the same small groups when rack awareness is in use, every
 *     topic is compared with the exact optimum of its partition ids for the allocations of the
 *     result, found with a minimum cost flow: the largest number of aligned partitions, and the
 *     fewest moves among the assignments reaching it. A topic reaching that alignment moves at
 *     most {@link #MAX_RACK_AWARE_EXCESS} partitions more than the fewest, and over the whole
 *     run the moves beyond the fewest are at most 5% of the topics checked, or 10. The excess
 *     comes from the maximum flow of the align step, which does not look at who could take a
 *     leftover back when it chooses between the groups of partitions able to serve a rack, nor
 *     between the members of a rack: a member whose deficit its own unalignable partitions
 *     could cover may be served by the flow, and those partitions then have to move. The
 *     summary line counts the topics moving more than the fewest and the moves in excess, as
 *     well as the topics below their alignment optimum, which are not checked for moves;</li>
 *     <li>rack aware on demand: with rack awareness disabled, the racks do not change the result;
 *     enabled but with a member without rack, or a single rack, the result is the plain one;
 *     enabled and usable, the number of aligned partitions is compared with an upper bound
 *     computed with a maximum flow per topic over the per member allocations of the result. The bound
 *     is conditional on those allocations: it judges the choice of the partition ids, not the choice
 *     of the members getting the extra partitions. At every step the gap is at most
 *     {@link #MAX_ALIGNMENT_GAP} partitions, and over the whole run it is at most 0.1% of the
 *     bound, or 10 partitions, since the swap step only exchanges pairs of partitions and misses
 *     the rare improvements needing a longer chain.</li>
 * </ul>
 *
 * <p>System properties: {@code uniform2.fuzz.seeds} sets the number of seeds (2000 by default),
 * {@code uniform2.fuzz.seed} runs a single seed, {@code uniform2.fuzz.events} the number of
 * events per seed (12 by default), {@code uniform2.fuzz.oracle=false} skips the brute force
 * oracle, and {@code uniform2.fuzz.reference=true} also runs the {@link UniformAssignor} on the
 * same inputs, only to compare the moved and aligned partitions in the summary line. Gradle does
 * not forward {@code -D} options to the test JVM, so pass them through the
 * {@code JAVA_TOOL_OPTIONS} environment variable, for instance
 * {@code JAVA_TOOL_OPTIONS=-Duniform2.fuzz.seeds=20000 ./gradlew :group-coordinator:test --rerun --tests ...}.
 * Every failure message carries the seed, the step, the event and the dump of the scenario as the
 * assignor saw it at that step: after the event, before the assignment was applied.
 */
public class Uniform2AssignorFuzzTest {
    private static final int SEEDS = Integer.getInteger("uniform2.fuzz.seeds", 2000);
    private static final Integer SEED = Integer.getInteger("uniform2.fuzz.seed");
    private static final int EVENTS = Integer.getInteger("uniform2.fuzz.events", 12);
    /** The largest groups the brute force oracle handles. */
    private static final int ORACLE_MAX_MEMBERS = 4;
    private static final int ORACLE_MAX_TOPICS = 4;
    /** The largest gap tolerated at a single step between the aligned partitions and their upper bound. */
    private static final int MAX_ALIGNMENT_GAP = 8;
    /** The most moves a topic reaching its best alignment may make beyond the fewest for that alignment. */
    private static final int MAX_RACK_AWARE_EXCESS = 8;
    /** Lets a run skip the brute force oracle, which dominates the run time of tiny groups. */
    private static final boolean ORACLE = Boolean.parseBoolean(System.getProperty("uniform2.fuzz.oracle", "true"));
    /** Whether the {@link UniformAssignor} runs on the same inputs, for the summary line only. */
    private static final boolean REFERENCE = Boolean.parseBoolean(System.getProperty("uniform2.fuzz.reference", "false"));

    @ParameterizedTest(name = "rackAware={0}")
    @ValueSource(booleans = {false, true})
    public void testRandomScenarios(boolean rackAware) {
        Harness harness = new Harness(rackAware);
        int first = SEED == null ? 0 : SEED;
        int last = SEED == null ? SEEDS - 1 : SEED;
        for (int seed = first; seed <= last; seed++) {
            Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(seed);
            harness.check(scenario, 0, Event.INIT);
            for (int step = 1; step <= EVENTS; step++) {
                harness.check(scenario, step, scenario.mutate());
            }
        }
        harness.assertAlignment();
        harness.assertRackAwareMovement();
        System.out.println(harness.summary());
    }

    /**
     * Runs the checks of one step and accumulates the statistics of the run.
     */
    private static final class Harness {
        private final boolean rackAware;
        private final Uniform2Assignor assignor;
        private final Uniform2Assignor plainAssignor = new Uniform2Assignor(false);
        private final UniformAssignor reference = new UniformAssignor();
        private long assignments;
        private long moved;
        private long oracleChecks;
        /** Oracle checks that needed one move more than the minimum, with a single subscription. */
        private long oracleExcessHomogeneous;
        /** Oracle checks that needed one move more than the minimum, with several subscriptions. */
        private long oracleExcessHeterogeneous;
        /** Rack oracle checks: topics of tiny groups using racks with as many aligned partitions as possible. */
        private long rackOracleChecks;
        /** Rack oracle checks that moved more partitions than the fewest for their alignment. */
        private long rackOracleExcess;
        /** The moves of the rack oracle checks beyond the fewest for their alignment, added up. */
        private long rackOracleExcessMoves;
        private long rackOracleWorstExcess;
        /** Topics of tiny groups using racks with fewer aligned partitions than possible, not checked for moves. */
        private long rackOracleBelowAlignment;
        private long aligned;
        private long alignedBound;
        private long worstAlignmentGap;
        private String worstAlignmentGapContext = "";
        private long referenceMoved;
        private long referenceAligned;
        private long referenceFailures;

        Harness(boolean rackAware) {
            this.rackAware = rackAware;
            this.assignor = new Uniform2Assignor(rackAware);
        }

        void check(Uniform2FuzzScenario scenario, int step, Event event) {
            String context = "seed=" + scenario.seed() + " step=" + step + " event=\"" + event + "\" rackAware=" + rackAware;
            try {
                step(scenario, event, context);
            } catch (AssertionError | RuntimeException e) {
                String message = String.valueOf(e.getMessage());
                String prefix = message.startsWith(context) ? "" : context + ": ";
                fail(prefix + message + System.lineSeparator() + scenario.dump(), e);
            }
        }

        private void step(Uniform2FuzzScenario scenario, Event event, String context) {
            Map<String, MemberSubscriptionAndAssignmentImpl> members = scenario.members();
            SubscribedTopicDescriber describer = scenario.describer();
            GroupSpec spec = spec(members);
            GroupAssignment result = assignor.assign(spec, describer);
            assignments++;

            assertValidAssignment(members, describer, result, context);
            assertOrderIndependent(members, describer, result, context);
            assertFixedPoint(members, describer, result, context);
            boolean usesRacks = assertRackModes(members, describer, spec, result, context);

            int movedNow = revocations(members, result);
            moved += movedNow;
            if (!usesRacks) {
                assertMovementBounds(members, describer, result, event, movedNow, context);
                assertMinimalMovement(members, describer, movedNow, context);
            } else {
                assertMinimalRackAwareMovement(members, describer, result, context);
            }
            boolean countAlignment = rackAware && allRacked(members);
            if (countAlignment) {
                recordAlignment(members, describer, result, scenario, context);
            }
            if (REFERENCE) {
                compareWithReference(spec, describer, members, countAlignment);
            }
            scenario.apply(result);
        }

        /**
         * The members, the topics of their subscriptions and the entries of their current
         * assignments given in reverse order produce the same assignment. The scenario hands
         * them out in ascending order, so descending order is the opposite of the order the
         * assignor saw.
         */
        private void assertOrderIndependent(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            String context
        ) {
            List<String> ids = new ArrayList<>(members.keySet());
            Collections.reverse(ids);
            Map<String, MemberSubscriptionAndAssignmentImpl> reversed = new LinkedHashMap<>();
            for (String id : ids) {
                MemberSubscriptionAndAssignmentImpl member = members.get(id);
                reversed.put(id, new MemberSubscriptionAndAssignmentImpl(
                    member.rackId(),
                    member.instanceId(),
                    descending(member.subscribedTopicIds()),
                    new Assignment(descending(member.partitions()))
                ));
            }
            GroupAssignment reversedResult = assignor.assign(spec(reversed), describer);
            assertEquals(result, reversedResult,
                context + ": the order of the members, topics or partitions changed the assignment");
        }

        /**
         * Feeding the assignment back returns the very same assignment maps, hence the very same
         * partition sets.
         */
        private void assertFixedPoint(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            String context
        ) {
            Map<String, MemberSubscriptionAndAssignmentImpl> again = withAssignment(members, result);
            GroupAssignment second = assignor.assign(spec(again), describer);
            for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : again.entrySet()) {
                assertSame(entry.getValue().partitions(), second.members().get(entry.getKey()).partitions(),
                    context + ": the assignment of " + entry.getKey() + " is not a fixed point");
            }
        }

        /**
         * Racks are ignored when rack awareness is disabled, and cannot be used when a member has
         * no rack or all members are in the same rack.
         *
         * @return Whether rack awareness is in use.
         */
        private boolean assertRackModes(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupSpec spec,
            GroupAssignment result,
            String context
        ) {
            Set<String> racks = new TreeSet<>();
            members.values().forEach(member -> member.rackId().ifPresent(racks::add));
            if (!rackAware) {
                if (!racks.isEmpty()) {
                    GroupAssignment withoutRacks = assignor.assign(spec(withoutRacks(members)), describer);
                    assertEquals(result, withoutRacks, context + ": the racks changed the assignment with rack awareness disabled");
                }
                return false;
            }
            if (allRacked(members) && racks.size() >= 2) {
                return true;
            }
            GroupAssignment plain = plainAssignor.assign(spec, describer);
            assertEquals(result, plain, context + ": the assignment differs from the plain one although rack awareness cannot be used");
            return false;
        }

        /**
         * With a single subscription, the partitions moved among the members present both before
         * and after a single leave or join are at most the extra partitions of the group. A leave
         * only frees the partitions of the leaver and a join only takes what the joiner is owed,
         * except that the change of allocations can leave the other members uneven: the even out phase
         * then moves backed extra partitions, each costing one move and each moving at most once.
         */
        private void assertMovementBounds(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            Event event,
            int movedNow,
            String context
        ) {
            if (subscriptionType(members) != SubscriptionType.HOMOGENEOUS) {
                return;
            }
            if (event.kind() == Kind.LEAVE) {
                int bound = totalExtraPartitions(members, describer);
                assertTrue(movedNow <= bound, context + ": a single leave moved " + movedNow
                    + " partitions among the remaining members, more than the " + bound + " extra partitions");
            } else if (event.kind() == Kind.JOIN) {
                int intake = load(result, event.members().get(0));
                int bound = totalExtraPartitions(members, describer);
                assertTrue(movedNow - intake <= bound, context + ": a single join moved " + movedNow
                    + " partitions for an intake of " + intake + ", more than the " + bound + " extra partitions beyond it");
            }
        }

        /**
         * @return The number of extra partitions of the group: for every subscribed topic, its
         *         partition count modulo its number of subscribers.
         */
        private static int totalExtraPartitions(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer
        ) {
            Map<Uuid, Integer> subscribers = new HashMap<>();
            members.values().forEach(member -> member.subscribedTopicIds().forEach(topicId -> subscribers.merge(topicId, 1, Integer::sum)));
            int total = 0;
            for (Map.Entry<Uuid, Integer> entry : subscribers.entrySet()) {
                total += describer.numPartitions(entry.getKey()) % entry.getValue();
            }
            return total;
        }

        /**
         * For tiny groups, at most one partition more than the fewest moves of any assignment
         * with the properties. The fill and even out phases settle ties without looking ahead, so
         * they occasionally reach a balance costing one move more than another one.
         */
        private void assertMinimalMovement(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            int movedNow,
            String context
        ) {
            Integer minimum = MovementOracle.minimalMovement(members, describer);
            if (minimum == null) {
                return;
            }
            oracleChecks++;
            assertTrue(movedNow >= minimum, context + ": " + movedNow + " partitions moved but the oracle needs " + minimum);
            assertTrue(movedNow - minimum <= 1, context + ": " + movedNow + " partitions moved where " + minimum + " suffice");
            if (movedNow > minimum) {
                if (subscriptionType(members) == SubscriptionType.HOMOGENEOUS) {
                    oracleExcessHomogeneous++;
                } else {
                    oracleExcessHeterogeneous++;
                }
            }
        }

        /**
         * For tiny groups using racks, checks the choice of the partition ids topic by topic
         * against the exact optimum of {@link RackAwareOracle} for the allocations of the result. A
         * topic having as many aligned partitions as possible moves at most
         * {@link #MAX_RACK_AWARE_EXCESS} partitions more than the fewest possible at that
         * alignment. A topic below its best alignment is only counted: the alignment bound
         * covers it, and the fewest moves at a lower alignment is another question.
         */
        private void assertMinimalRackAwareMovement(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            String context
        ) {
            List<Uuid> topics = subscribedTopics(members);
            if (!ORACLE || members.size() > ORACLE_MAX_MEMBERS || topics.size() > ORACLE_MAX_TOPICS) {
                return;
            }
            for (Uuid topicId : topics) {
                RackAwareOracle.Optimum optimum = RackAwareOracle.optimum(members, describer, result, topicId);
                int alignedNow = topicAlignedPartitions(members, result, describer, topicId);
                int movedNow = topicMovedPartitions(members, result, describer, topicId);
                String topicContext = context + ": topic " + topicId + " has " + alignedNow
                    + " aligned partitions and moved " + movedNow;
                assertTrue(alignedNow <= optimum.aligned(), topicContext + ", more than the optimum of " + optimum.aligned());
                if (alignedNow < optimum.aligned()) {
                    rackOracleBelowAlignment++;
                    continue;
                }
                rackOracleChecks++;
                assertTrue(movedNow >= optimum.moves(), topicContext + " but the oracle needs " + optimum.moves());
                assertTrue(movedNow - optimum.moves() <= MAX_RACK_AWARE_EXCESS, topicContext + " where " + optimum.moves() + " suffice");
                if (movedNow > optimum.moves()) {
                    rackOracleExcess++;
                    rackOracleExcessMoves += movedNow - optimum.moves();
                    rackOracleWorstExcess = Math.max(rackOracleWorstExcess, movedNow - optimum.moves());
                }
            }
        }

        /**
         * Records the aligned partitions of the step and their upper bound, and checks that the
         * gap of this step stays within {@link #MAX_ALIGNMENT_GAP}.
         */
        private void recordAlignment(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            Uniform2FuzzScenario scenario,
            String context
        ) {
            long alignedNow = alignedPartitions(members, result, describer);
            long boundNow = alignmentUpperBound(members, describer, result);
            aligned += alignedNow;
            alignedBound += boundNow;
            assertTrue(boundNow - alignedNow <= MAX_ALIGNMENT_GAP,
                context + ": " + alignedNow + " aligned partitions for a bound of " + boundNow);
            if (boundNow - alignedNow > worstAlignmentGap) {
                worstAlignmentGap = boundNow - alignedNow;
                worstAlignmentGapContext = context + ": " + alignedNow + " aligned partitions for a bound of " + boundNow
                    + System.lineSeparator() + scenario.dump();
            }
        }

        /**
         * Runs the {@link UniformAssignor} on the same input and accumulates its moved and aligned
         * partitions for the summary line. Nothing is asserted: it is only a point of comparison,
         * and its failures are counted rather than reported.
         */
        private void compareWithReference(
            GroupSpec spec,
            SubscribedTopicDescriber describer,
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            boolean countAlignment
        ) {
            try {
                GroupAssignment referenceResult = reference.assign(spec, describer);
                referenceMoved += revocations(members, referenceResult);
                if (countAlignment) {
                    referenceAligned += alignedPartitions(members, referenceResult, describer);
                }
            } catch (RuntimeException e) {
                referenceFailures++;
            }
        }

        /**
         * Over the whole run, the aligned partitions are within 0.1% of their upper bound, or
         * within 10 partitions when the bound is small, as in a single seed run: the swap step
         * only exchanges pairs of partitions, so it can miss the rare improvements needing a
         * longer chain.
         */
        void assertAlignment() {
            assertTrue(alignedBound - aligned <= Math.max(10, alignedBound / 1000),
                "rack alignment " + aligned + " is too far below the upper bound " + alignedBound
                    + ", worst step: " + worstAlignmentGapContext);
        }

        /**
         * Over the whole run, the topics checked against the rack oracle moved at most 5% of
         * their number in excess of the fewest moves, or 10 when few were checked, as in a
         * single seed run: the flow does not look at who could take a leftover back when it
         * picks the groups serving a rack and the members it serves within a rack.
         */
        void assertRackAwareMovement() {
            assertTrue(rackOracleExcessMoves <= Math.max(10, rackOracleChecks / 20),
                "rack aware topics moved " + rackOracleExcessMoves + " partitions beyond the fewest over "
                    + rackOracleChecks + " checks, worst excess " + rackOracleWorstExcess);
        }

        String summary() {
            String summary = String.format("uniform2 fuzz rackAware=%s: %d assignments (%d seeds, 1 initial + %d events each), "
                    + "moved partitions=%d, oracle checks=%d (one move above the minimum: homogeneous=%d heterogeneous=%d), "
                    + "rack oracle checks=%d (topics above the fewest moves=%d, moves in excess=%d, worst=%d, "
                    + "topics below the alignment=%d), aligned partitions=%d for a bound of %d",
                rackAware, assignments, SEED == null ? SEEDS : 1, EVENTS, moved, oracleChecks, oracleExcessHomogeneous,
                oracleExcessHeterogeneous, rackOracleChecks, rackOracleExcess, rackOracleExcessMoves, rackOracleWorstExcess,
                rackOracleBelowAlignment, aligned, alignedBound);
            if (REFERENCE) {
                summary += String.format(", uniform: moved partitions=%d aligned partitions=%d failures=%d",
                    referenceMoved, referenceAligned, referenceFailures);
            }
            return summary;
        }
    }

    private static boolean allRacked(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        return members.values().stream().allMatch(member -> member.rackId().isPresent());
    }

    private static Map<String, MemberSubscriptionAndAssignmentImpl> withoutRacks(
        Map<String, MemberSubscriptionAndAssignmentImpl> members
    ) {
        Map<String, MemberSubscriptionAndAssignmentImpl> result = new TreeMap<>();
        members.forEach((id, member) -> result.put(id, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            member.instanceId(),
            member.subscribedTopicIds(),
            new Assignment(member.partitions())
        )));
        return result;
    }

    /**
     * @return The elements in descending order.
     */
    private static <T extends Comparable<T>> Set<T> descending(Set<T> elements) {
        List<T> sorted = new ArrayList<>(elements);
        sorted.sort(Collections.reverseOrder());
        return new LinkedHashSet<>(sorted);
    }

    /**
     * @return The assignment with its topics and the partitions of every topic in descending order.
     */
    private static Map<Uuid, Set<Integer>> descending(Map<Uuid, Set<Integer>> partitions) {
        Map<Uuid, Set<Integer>> result = new LinkedHashMap<>();
        for (Uuid topicId : descending(partitions.keySet())) {
            result.put(topicId, descending(partitions.get(topicId)));
        }
        return result;
    }

    /**
     * @return The topics subscribed by at least one member, sorted.
     */
    private static List<Uuid> subscribedTopics(Map<String, MemberSubscriptionAndAssignmentImpl> members) {
        Set<Uuid> topics = new TreeSet<>();
        members.values().forEach(member -> topics.addAll(member.subscribedTopicIds()));
        return new ArrayList<>(topics);
    }

    /**
     * @return The number of partitions of the topic having a replica in the rack of their member.
     */
    private static int topicAlignedPartitions(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        GroupAssignment result,
        SubscribedTopicDescriber describer,
        Uuid topicId
    ) {
        int aligned = 0;
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : members.entrySet()) {
            String rack = entry.getValue().rackId().orElseThrow();
            for (int partition : result.members().get(entry.getKey()).partitions().getOrDefault(topicId, Set.of())) {
                if (describer.racksForPartition(topicId, partition).contains(rack)) {
                    aligned++;
                }
            }
        }
        return aligned;
    }

    /**
     * @return The number of existing partitions of the topic that a member holds and does not get back.
     */
    private static int topicMovedPartitions(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        GroupAssignment result,
        SubscribedTopicDescriber describer,
        Uuid topicId
    ) {
        int numPartitions = describer.numPartitions(topicId);
        int moved = 0;
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : members.entrySet()) {
            Set<Integer> kept = result.members().get(entry.getKey()).partitions().getOrDefault(topicId, Set.of());
            for (int partition : entry.getValue().partitions().getOrDefault(topicId, Set.of())) {
                if (partition < numPartitions && !kept.contains(partition)) {
                    moved++;
                }
            }
        }
        return moved;
    }

    /**
     * The exact optimum of the partition ids of one topic with racks, for the allocations of an
     * assignment: the largest number of aligned partitions, and the fewest moved partitions among
     * the assignments reaching it. It is a minimum cost flow from the partitions to the
     * subscribers, each taking its allocation, in which giving a partition to a member without a
     * replica in its rack costs far more than taking it away from its current owner. A
     * partition nobody holds costs nothing to anyone, and one held by a member who is not a
     * subscriber of the topic costs the same move to everyone. Members having left hold nothing.
     */
    private static final class RackAwareOracle {
        private static final int MISALIGNED_COST = 1 << 20;

        /** The largest number of aligned partitions, and the fewest moves among the assignments reaching it. */
        record Optimum(int aligned, int moves) { }

        static Optimum optimum(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            Uuid topicId
        ) {
            int partitions = describer.numPartitions(topicId);
            List<String> ids = new ArrayList<>(members.keySet());
            int source = partitions + ids.size();
            int sink = source + 1;
            MinCostFlow flow = new MinCostFlow(sink + 1, partitions + partitions * ids.size() + ids.size());
            for (int m = 0; m < ids.size(); m++) {
                int allocation = result.members().get(ids.get(m)).partitions().getOrDefault(topicId, Set.of()).size();
                if (allocation > 0) {
                    flow.addEdge(partitions + m, sink, allocation, 0);
                }
            }
            Map<Integer, String> owners = invertedTargetAssignment(members).getOrDefault(topicId, Map.of());
            for (int p = 0; p < partitions; p++) {
                flow.addEdge(source, p, 1, 0);
                Set<String> racks = describer.racksForPartition(topicId, p);
                String owner = owners.get(p);
                for (int m = 0; m < ids.size(); m++) {
                    MemberSubscriptionAndAssignmentImpl member = members.get(ids.get(m));
                    if (!member.subscribedTopicIds().contains(topicId)) {
                        continue;
                    }
                    int cost = racks.contains(member.rackId().orElseThrow()) ? 0 : MISALIGNED_COST;
                    if (owner != null && !owner.equals(ids.get(m))) {
                        cost++;
                    }
                    flow.addEdge(p, partitions + m, 1, cost);
                }
            }
            long cost = flow.send(source, sink, partitions);
            return new Optimum(partitions - (int) (cost / MISALIGNED_COST), (int) (cost % MISALIGNED_COST));
        }
    }

    /**
     * A minimum cost flow on a small network by successive shortest paths: every unit goes along
     * the cheapest path of the residual network, found with a Bellman-Ford search since the
     * reverse edges have negative costs. Every path carries one unit here, the edges out of the
     * source having a capacity of one.
     */
    private static final class MinCostFlow {
        private final int[] head;
        private final int[] next;
        private final int[] to;
        private final int[] capacity;
        private final int[] cost;
        private int edges;

        MinCostFlow(int nodes, int maxEdges) {
            head = new int[nodes];
            Arrays.fill(head, -1);
            next = new int[2 * maxEdges];
            to = new int[2 * maxEdges];
            capacity = new int[2 * maxEdges];
            cost = new int[2 * maxEdges];
        }

        /**
         * Adds an edge and its reverse right after it, so that {@code e ^ 1} is the reverse of {@code e}.
         */
        void addEdge(int from, int target, int edgeCapacity, int unitCost) {
            add(from, target, edgeCapacity, unitCost);
            add(target, from, 0, -unitCost);
        }

        private void add(int from, int target, int edgeCapacity, int unitCost) {
            to[edges] = target;
            capacity[edges] = edgeCapacity;
            cost[edges] = unitCost;
            next[edges] = head[from];
            head[from] = edges++;
        }

        /**
         * @return The cost of sending the amount from the source to the sink, one unit at a time.
         */
        long send(int source, int sink, int amount) {
            int nodes = head.length;
            long[] distance = new long[nodes];
            int[] parentEdge = new int[nodes];
            boolean[] queued = new boolean[nodes];
            long total = 0;
            for (int sent = 0; sent < amount; sent++) {
                Arrays.fill(distance, Long.MAX_VALUE);
                Arrays.fill(parentEdge, -1);
                distance[source] = 0;
                ArrayDeque<Integer> queue = new ArrayDeque<>();
                queue.add(source);
                while (!queue.isEmpty()) {
                    int u = queue.poll();
                    queued[u] = false;
                    for (int e = head[u]; e != -1; e = next[e]) {
                        int v = to[e];
                        if (capacity[e] > 0 && distance[u] + cost[e] < distance[v]) {
                            distance[v] = distance[u] + cost[e];
                            parentEdge[v] = e;
                            if (!queued[v]) {
                                queued[v] = true;
                                queue.add(v);
                            }
                        }
                    }
                }
                if (parentEdge[sink] == -1) {
                    throw new IllegalStateException("The allocations do not cover the partitions.");
                }
                for (int v = sink; v != source; v = to[parentEdge[v] ^ 1]) {
                    capacity[parentEdge[v]]--;
                    capacity[parentEdge[v] ^ 1]++;
                }
                total += distance[sink];
            }
            return total;
        }
    }

    /**
     * @return The largest number of aligned partitions any assignment with the per member allocations
     *         of the result could have. Per topic, a maximum flow sends the partitions, grouped
     *         by their set of replica racks, through the racks to the members of each rack, whose
     *         demand is their allocation. The bound is conditional on the allocations of the result: it says
     *         how well the partition ids were chosen, not whether other members should have
     *         received the extra partitions.
     */
    private static long alignmentUpperBound(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscribedTopicDescriber describer,
        GroupAssignment result
    ) {
        List<String> racks = new ArrayList<>(new TreeSet<>(
            members.values().stream().map(member -> member.rackId().orElseThrow()).toList()));
        long bound = 0;
        for (Uuid topicId : subscribedTopics(members)) {
            int[] demand = new int[racks.size()];
            members.forEach((id, member) -> demand[racks.indexOf(member.rackId().orElseThrow())] +=
                result.members().get(id).partitions().getOrDefault(topicId, Set.of()).size());
            Map<Integer, Integer> groups = new HashMap<>();
            for (int partition = 0; partition < describer.numPartitions(topicId); partition++) {
                int mask = 0;
                for (String rack : describer.racksForPartition(topicId, partition)) {
                    int index = racks.indexOf(rack);
                    if (index >= 0) {
                        mask |= 1 << index;
                    }
                }
                if (mask != 0) {
                    groups.merge(mask, 1, Integer::sum);
                }
            }
            bound += maxAlignment(groups, demand);
        }
        return bound;
    }

    /**
     * @param groups Per set of racks, as a bit mask over the racks, the number of partitions
     *               having their replicas in exactly these racks.
     * @param demand Per rack, the number of partitions its members get.
     * @return The largest number of partitions that can go to a member in one of their racks.
     */
    private static int maxAlignment(Map<Integer, Integer> groups, int[] demand) {
        int rackCount = demand.length;
        int groupCount = groups.size();
        int source = 0;
        int firstRack = 1 + groupCount;
        int sink = firstRack + rackCount;
        int[][] capacity = new int[sink + 1][sink + 1];
        int group = 1;
        for (Map.Entry<Integer, Integer> entry : groups.entrySet()) {
            capacity[source][group] = entry.getValue();
            for (int rack = 0; rack < rackCount; rack++) {
                if ((entry.getKey() & (1 << rack)) != 0) {
                    capacity[group][firstRack + rack] = entry.getValue();
                }
            }
            group++;
        }
        for (int rack = 0; rack < rackCount; rack++) {
            capacity[firstRack + rack][sink] = demand[rack];
        }
        return maxFlow(capacity, source, sink);
    }

    /**
     * Edmonds-Karp on a capacity matrix.
     */
    private static int maxFlow(int[][] capacity, int source, int sink) {
        int nodes = capacity.length;
        int flow = 0;
        int[] parent = new int[nodes];
        while (true) {
            Arrays.fill(parent, -1);
            parent[source] = source;
            ArrayDeque<Integer> queue = new ArrayDeque<>();
            queue.add(source);
            while (!queue.isEmpty() && parent[sink] == -1) {
                int u = queue.poll();
                for (int v = 0; v < nodes; v++) {
                    if (parent[v] == -1 && capacity[u][v] > 0) {
                        parent[v] = u;
                        queue.add(v);
                    }
                }
            }
            if (parent[sink] == -1) {
                return flow;
            }
            int bottleneck = Integer.MAX_VALUE;
            for (int v = sink; v != source; v = parent[v]) {
                bottleneck = Math.min(bottleneck, capacity[parent[v]][v]);
            }
            for (int v = sink; v != source; v = parent[v]) {
                capacity[parent[v]][v] -= bottleneck;
                capacity[v][parent[v]] += bottleneck;
            }
            flow += bottleneck;
        }
    }

    /**
     * Brute force computation of the smallest number of partitions that an assignment with the
     * spread and balance properties must move from the current assignment.
     *
     * <p>The spread fixes the allocation of every subscriber of a topic to the base partitions or one
     * more, so an assignment is characterized, up to partition ids, by which subscribers get the
     * extra partitions of each topic. For given allocations, the fewest moves are made when every
     * member keeps as many of its current partitions of each topic as its allocation allows, which is
     * always possible without racks. The oracle enumerates all the ways to hand out the extra
     * partitions, keeps the balanced ones and takes the cheapest. Current partitions of topics a
     * member is not subscribed to, or beyond the partition count, are lost whatever the allocations.
     * Balance means all loads within one of each other with a single subscription, and
     * otherwise that no extra partition could move to a subscriber at least two below its owner.
     */
    private static final class MovementOracle {
        private final int memberCount;
        private final int topicCount;
        private final boolean homogeneous;
        /** Per topic, its subscribers as a bit mask over the members. */
        private final int[] subscribers;
        private final int[] base;
        private final int[] extras;
        /** Per topic and member, the current partitions of the member that the allocation can keep. */
        private final int[][] current;
        /** The current partitions lost whatever the allocations. */
        private final int lost;
        /** Search state: per topic, the members getting an extra partition, and the loads. */
        private final int[] extraMasks;
        private final int[] loads;
        private int best = Integer.MAX_VALUE;

        /**
         * @return The smallest number of moved partitions, or null when the group is too large.
         */
        static Integer minimalMovement(
            Map<String, MemberSubscriptionAndAssignmentImpl> members,
            SubscribedTopicDescriber describer
        ) {
            List<Uuid> topics = subscribedTopics(members);
            if (!ORACLE || members.isEmpty() || members.size() > ORACLE_MAX_MEMBERS || topics.size() > ORACLE_MAX_TOPICS) {
                return null;
            }
            boolean homogeneous = subscriptionType(members) == SubscriptionType.HOMOGENEOUS;
            MovementOracle oracle = new MovementOracle(new ArrayList<>(members.values()), topics, describer, homogeneous);
            oracle.search(0, 0);
            return oracle.lost + oracle.best;
        }

        private MovementOracle(
            List<MemberSubscriptionAndAssignmentImpl> members,
            List<Uuid> topics,
            SubscribedTopicDescriber describer,
            boolean homogeneous
        ) {
            memberCount = members.size();
            topicCount = topics.size();
            this.homogeneous = homogeneous;
            subscribers = new int[topicCount];
            base = new int[topicCount];
            extras = new int[topicCount];
            current = new int[topicCount][memberCount];
            extraMasks = new int[topicCount];
            loads = new int[memberCount];
            int lostPartitions = 0;
            for (int t = 0; t < topicCount; t++) {
                Uuid topicId = topics.get(t);
                int partitions = describer.numPartitions(topicId);
                for (int m = 0; m < memberCount; m++) {
                    MemberSubscriptionAndAssignmentImpl member = members.get(m);
                    boolean subscribed = member.subscribedTopicIds().contains(topicId);
                    subscribers[t] |= subscribed ? 1 << m : 0;
                    for (int partition : member.partitions().getOrDefault(topicId, Set.of())) {
                        if (subscribed && partition < partitions) {
                            current[t][m]++;
                        } else {
                            lostPartitions++;
                        }
                    }
                }
                int subscriberCount = Integer.bitCount(subscribers[t]);
                base[t] = partitions / subscriberCount;
                extras[t] = partitions % subscriberCount;
            }
            for (MemberSubscriptionAndAssignmentImpl member : members) {
                for (Map.Entry<Uuid, Set<Integer>> entry : member.partitions().entrySet()) {
                    if (!topics.contains(entry.getKey())) {
                        lostPartitions += entry.getValue().size();
                    }
                }
            }
            lost = lostPartitions;
        }

        /**
         * Tries every way to hand out the extra partitions of the topics from {@code t} on.
         */
        private void search(int t, int moves) {
            if (t == topicCount) {
                if (balanced()) {
                    best = Math.min(best, moves);
                }
                return;
            }
            for (int mask = 0; mask < 1 << memberCount; mask++) {
                if (Integer.bitCount(mask) != extras[t] || (mask & ~subscribers[t]) != 0) {
                    continue;
                }
                extraMasks[t] = mask;
                int topicMoves = 0;
                for (int m = 0; m < memberCount; m++) {
                    if ((subscribers[t] & (1 << m)) != 0) {
                        int allocation = base[t] + ((mask >> m) & 1);
                        loads[m] += allocation;
                        topicMoves += Math.max(0, current[t][m] - allocation);
                    }
                }
                search(t + 1, moves + topicMoves);
                for (int m = 0; m < memberCount; m++) {
                    if ((subscribers[t] & (1 << m)) != 0) {
                        loads[m] -= base[t] + ((mask >> m) & 1);
                    }
                }
            }
        }

        private boolean balanced() {
            if (homogeneous) {
                return Arrays.stream(loads).max().orElseThrow() - Arrays.stream(loads).min().orElseThrow() <= 1;
            }
            for (int t = 0; t < topicCount; t++) {
                for (int giver = 0; giver < memberCount; giver++) {
                    if ((extraMasks[t] & (1 << giver)) == 0) {
                        continue;
                    }
                    for (int receiver = 0; receiver < memberCount; receiver++) {
                        boolean eligible = (subscribers[t] & (1 << receiver)) != 0 && (extraMasks[t] & (1 << receiver)) == 0;
                        if (eligible && loads[giver] >= loads[receiver] + 2) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
