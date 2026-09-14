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
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A random consumer group and cluster for the uniform2 fuzzer, with the events that change them.
 *
 * <p>A scenario is entirely determined by its seed. It has topics, each with a number of
 * partitions and the racks of the replicas of every partition, cohorts, which are the
 * subscriptions that joining members copy, and members with a rack, a subscription and the
 * partitions they currently hold. {@link #mutate()} applies one random event: members join or
 * leave, alone or several at once, a topic gains partitions, a cohort subscribes to or drops a
 * topic, a member toggles a topic of its own, replicas move, a member changes rack. The current
 * partitions are only changed through {@link #apply(GroupAssignment)}, so the events are
 * independent of the assignor under test.
 *
 * <p>Scenarios come in three sizes: tiny groups where the edge cases live, medium groups, and
 * large skewed groups with many single partition topics and a few large ones. Members may all
 * have a rack, only some of them, or none at first; a member changing rack later may gain or
 * lose one, unless every member must have one. Replicas are spread over the three broker racks
 * in sets of one to three racks, and a few members live in a rack without brokers.
 *
 * <p>{@link #dump()} prints the whole scenario, so that a failure can be reproduced in a unit
 * test from the seed, the step and the dump alone.
 */
final class Uniform2FuzzScenario {
    /** The racks having brokers, so the racks where replicas can be. */
    static final List<String> BROKER_RACKS = List.of("a", "b", "c");
    /** A rack having members but no broker, so no replica. */
    static final String BROKERLESS_RACK = "d";

    /** How big the group and the topics are. */
    enum Size { TINY, MEDIUM, LARGE }

    /** Which members have a rack. */
    enum RackMode { ALL, SOME, NONE }

    /** The kinds of events. */
    enum Kind {
        INIT,
        JOIN,
        LEAVE,
        JOIN_MANY,
        LEAVE_MANY,
        GROW_TOPIC,
        ADD_TOPIC_TO_COHORT,
        REMOVE_TOPIC_FROM_COHORT,
        TOGGLE_TOPIC,
        MOVE_REPLICAS,
        CHANGE_RACK
    }

    /**
     * An event, with the ids of the members it added or removed. A join or leave of exactly one
     * member has the kind {@link Kind#JOIN} or {@link Kind#LEAVE}, whatever the number asked; one
     * adding or removing nobody, because the group is full or empty, has the kind
     * {@link Kind#JOIN_MANY} or {@link Kind#LEAVE_MANY} and says so.
     */
    record Event(Kind kind, String description, List<String> members) {
        static final Event INIT = new Event(Kind.INIT, "init", List.of());

        @Override
        public String toString() {
            return description;
        }
    }

    private static final Kind[] EVENT_WEIGHTS = {
        Kind.JOIN, Kind.JOIN, Kind.JOIN,
        Kind.LEAVE, Kind.LEAVE, Kind.LEAVE,
        Kind.JOIN_MANY, Kind.LEAVE_MANY,
        Kind.GROW_TOPIC, Kind.GROW_TOPIC,
        Kind.ADD_TOPIC_TO_COHORT, Kind.REMOVE_TOPIC_FROM_COHORT, Kind.TOGGLE_TOPIC,
        Kind.MOVE_REPLICAS, Kind.CHANGE_RACK
    };
    private static final int[] REPLICATION_FACTORS = {1, 2, 2, 3};
    /** Caps keeping long event sequences bounded. */
    private static final int MAX_MEMBERS = 64;
    private static final int MAX_TOPICS = 48;
    private static final int MAX_PARTITIONS = 256;

    private static final class Topic {
        final String name;
        final Uuid id;
        final int replicationFactor;
        /** Per partition, the racks having a replica. Entries are replaced, never changed. */
        final List<Set<String>> replicaRacks = new ArrayList<>();

        Topic(String name, Uuid id, int replicationFactor) {
            this.name = name;
            this.id = id;
            this.replicationFactor = replicationFactor;
        }

        int partitions() {
            return replicaRacks.size();
        }
    }

    private static final class Member {
        final String id;
        final int cohort;
        String rack;
        final Set<Uuid> subscription = new HashSet<>();
        Map<Uuid, Set<Integer>> current = new TreeMap<>();

        Member(String id, int cohort, String rack) {
            this.id = id;
            this.cohort = cohort;
            this.rack = rack;
        }
    }

    /** A snapshot of the topics, as the assignor sees them. */
    private record Describer(Map<Uuid, List<Set<String>>> topics) implements SubscribedTopicDescriber {
        @Override
        public int numPartitions(Uuid topicId) {
            List<Set<String>> partitions = topics.get(topicId);
            return partitions == null ? -1 : partitions.size();
        }

        @Override
        public Set<String> racksForPartition(Uuid topicId, int partition) {
            List<Set<String>> partitions = topics.get(topicId);
            return partitions == null || partition < 0 || partition >= partitions.size() ? Set.of() : partitions.get(partition);
        }
    }

    private final long seed;
    private final Random random;
    private final Size size;
    private final RackMode rackMode;
    private final List<Topic> topics = new ArrayList<>();
    private final Map<Uuid, Topic> topicsById = new HashMap<>();
    /** Per cohort, its subscription. Members copy it when they join and follow its changes. */
    private final List<Set<Uuid>> cohorts = new ArrayList<>();
    private final TreeMap<String, Member> members = new TreeMap<>();

    Uniform2FuzzScenario(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.size = pick(Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY,
            Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM,
            Size.MEDIUM, Size.MEDIUM, Size.LARGE, Size.LARGE, Size.LARGE);
        this.rackMode = pick(RackMode.ALL, RackMode.ALL, RackMode.ALL, RackMode.SOME, RackMode.NONE);
        createTopics();
        createCohorts();
        int memberCount = switch (size) {
            case TINY -> 1 + random.nextInt(3);
            case MEDIUM -> 1 + random.nextInt(12);
            case LARGE -> 5 + random.nextInt(26);
        };
        for (int i = 0; i < memberCount; i++) {
            addMember(i % cohorts.size());
        }
    }

    long seed() {
        return seed;
    }

    Size size() {
        return size;
    }

    RackMode rackMode() {
        return rackMode;
    }

    int memberCount() {
        return members.size();
    }

    /**
     * @return The members, by id, as the assignor receives them: their rack, their subscription
     *         with the topics in ascending order, and a copy of their current partitions with the
     *         topics and partitions in ascending order, so that the input does not depend on hash
     *         iteration orders and a seed reproduces exactly. The fuzzer reverses these orders
     *         itself to check that they do not matter.
     */
    Map<String, MemberSubscriptionAndAssignmentImpl> members() {
        Map<String, MemberSubscriptionAndAssignmentImpl> result = new TreeMap<>();
        for (Member member : members.values()) {
            result.put(member.id, new MemberSubscriptionAndAssignmentImpl(
                Optional.ofNullable(member.rack),
                Optional.empty(),
                new TreeSet<>(member.subscription),
                new Assignment(copy(member.current))
            ));
        }
        return result;
    }

    /**
     * @return A snapshot of the topics: partition counts and replica racks.
     */
    SubscribedTopicDescriber describer() {
        Map<Uuid, List<Set<String>>> snapshot = new HashMap<>();
        for (Topic topic : topics) {
            snapshot.put(topic.id, List.copyOf(topic.replicaRacks));
        }
        return new Describer(snapshot);
    }

    /**
     * Makes the assignment the current one. Members absent from it hold nothing.
     */
    void apply(GroupAssignment assignment) {
        for (Member member : members.values()) {
            MemberAssignment memberAssignment = assignment.members().get(member.id);
            member.current = memberAssignment == null ? new TreeMap<>() : copy(memberAssignment.partitions());
        }
    }

    /**
     * Applies one random event.
     *
     * @return The event.
     */
    Event mutate() {
        Kind kind = EVENT_WEIGHTS[random.nextInt(EVENT_WEIGHTS.length)];
        boolean tiny = size == Size.TINY;
        return switch (kind) {
            case JOIN -> join(1);
            case JOIN_MANY -> join(2 + random.nextInt(tiny ? 1 : 3));
            case LEAVE -> leave(1);
            case LEAVE_MANY -> leave(2 + random.nextInt(tiny ? 1 : 3));
            case GROW_TOPIC -> growTopic();
            case ADD_TOPIC_TO_COHORT -> addTopicToCohort();
            case REMOVE_TOPIC_FROM_COHORT -> removeTopicFromCohort();
            case TOGGLE_TOPIC -> toggleTopic();
            case MOVE_REPLICAS -> moveReplicas();
            case CHANGE_RACK -> changeRack();
            default -> throw new IllegalStateException("Unexpected event kind " + kind);
        };
    }

    /**
     * @return The whole scenario: topics with their partitions and replica racks, cohorts, and
     *         members with their rack, subscription and current partitions.
     */
    String dump() {
        StringBuilder out = new StringBuilder();
        out.append("scenario seed=").append(seed).append(" size=").append(size).append(" racks=").append(rackMode)
            .append(" members=").append(members.size()).append(" topics=").append(topics.size()).append('\n');
        for (Topic topic : topics) {
            out.append("  topic ").append(topic.name).append(" id=").append(topic.id)
                .append(" partitions=").append(topic.partitions()).append(" replicaRacks=");
            for (int p = 0; p < topic.partitions(); p++) {
                out.append(p == 0 ? "[" : " ").append(String.join("", topic.replicaRacks.get(p)));
            }
            out.append(topic.partitions() == 0 ? "[]" : "]").append('\n');
        }
        for (int c = 0; c < cohorts.size(); c++) {
            out.append("  cohort ").append(c).append(" topics=").append(topicNames(cohorts.get(c))).append('\n');
        }
        for (Member member : members.values()) {
            out.append("  member ").append(member.id).append(" rack=").append(member.rack == null ? "-" : member.rack)
                .append(" cohort=").append(member.cohort).append(" topics=").append(topicNames(member.subscription))
                .append(" current=").append(currentPartitions(member.current)).append('\n');
        }
        return out.toString();
    }

    @Override
    public String toString() {
        return dump();
    }

    // Construction.

    private void createTopics() {
        int topicCount = switch (size) {
            case TINY -> 1 + random.nextInt(3);
            case MEDIUM -> 1 + random.nextInt(6);
            case LARGE -> 8 + random.nextInt(17);
        };
        int largeTopics = size == Size.LARGE ? 1 + random.nextInt(3) : 0;
        for (int i = 0; i < topicCount; i++) {
            createTopic(i < largeTopics ? 20 + random.nextInt(41) : initialPartitions());
        }
    }

    private int initialPartitions() {
        return switch (size) {
            case TINY -> 1 + random.nextInt(4);
            case MEDIUM -> 1 + random.nextInt(20);
            case LARGE -> 1;
        };
    }

    private Topic createTopic(int partitions) {
        Topic topic = new Topic(
            "T" + topics.size(),
            new Uuid(random.nextLong(), random.nextLong()),
            REPLICATION_FACTORS[random.nextInt(REPLICATION_FACTORS.length)]
        );
        for (int p = 0; p < partitions; p++) {
            topic.replicaRacks.add(replicaRacks(topic, p));
        }
        topics.add(topic);
        topicsById.put(topic.id, topic);
        return topic;
    }

    /**
     * @return The replica racks of a partition: the replication factor consecutive racks
     *         starting at the partition, or once in a while any set of one to three racks.
     */
    private Set<String> replicaRacks(Topic topic, int partition) {
        if (random.nextInt(10) == 0) {
            return randomRackSet();
        }
        Set<String> racks = new TreeSet<>();
        for (int r = 0; r < topic.replicationFactor; r++) {
            racks.add(BROKER_RACKS.get((partition + r) % BROKER_RACKS.size()));
        }
        return racks;
    }

    private Set<String> randomRackSet() {
        List<String> shuffled = new ArrayList<>(BROKER_RACKS);
        Collections.shuffle(shuffled, random);
        return new TreeSet<>(shuffled.subList(0, 1 + random.nextInt(BROKER_RACKS.size())));
    }

    /**
     * Creates one cohort, or two or three whose subscriptions overlap, nest or are disjoint.
     */
    private void createCohorts() {
        boolean homogeneous = random.nextBoolean();
        int cohortCount = homogeneous ? 1 : 2 + random.nextInt(2);
        Set<Uuid> first = homogeneous || random.nextBoolean() ? allTopics() : randomSubscription(allTopics());
        cohorts.add(first);
        for (int c = 1; c < cohortCount; c++) {
            Set<Uuid> others = allTopics();
            others.removeAll(first);
            cohorts.add(switch (random.nextInt(3)) {
                case 0 -> randomSubscription(allTopics());
                case 1 -> randomSubscription(first);
                default -> others.isEmpty() ? randomSubscription(allTopics()) : randomSubscription(others);
            });
        }
    }

    private Set<Uuid> allTopics() {
        Set<Uuid> all = new HashSet<>();
        for (Topic topic : topics) {
            all.add(topic.id);
        }
        return all;
    }

    /**
     * @return A random non-empty subset of the topics.
     */
    private Set<Uuid> randomSubscription(Set<Uuid> from) {
        List<Uuid> shuffled = sorted(from);
        Collections.shuffle(shuffled, random);
        return new HashSet<>(shuffled.subList(0, 1 + random.nextInt(shuffled.size())));
    }

    private Member addMember(int cohort) {
        Member member = new Member(newMemberId(), cohort, randomRack());
        member.subscription.addAll(cohorts.get(cohort));
        members.put(member.id, member);
        return member;
    }

    /**
     * @return An unused member id. Ids are drawn at random so that a joining member sorts
     *         anywhere among the existing ones.
     */
    private String newMemberId() {
        int bound = 100;
        while (true) {
            String id = String.format("m%02d", random.nextInt(bound));
            if (!members.containsKey(id)) {
                return id;
            }
            bound *= 10;
        }
    }

    private String randomRack() {
        return switch (rackMode) {
            case NONE -> null;
            case SOME -> random.nextInt(10) < 7 ? someRack() : null;
            case ALL -> someRack();
        };
    }

    private String someRack() {
        return random.nextInt(20) == 0 ? BROKERLESS_RACK : BROKER_RACKS.get(random.nextInt(BROKER_RACKS.size()));
    }

    // Events.

    private Event join(int count) {
        List<String> joined = new ArrayList<>();
        for (int i = 0; i < count && members.size() < MAX_MEMBERS; i++) {
            joined.add(addMember(random.nextInt(cohorts.size())).id);
        }
        if (joined.isEmpty()) {
            return new Event(Kind.JOIN_MANY, "nobody joins, the group is full", joined);
        }
        Kind kind = joined.size() == 1 ? Kind.JOIN : Kind.JOIN_MANY;
        return new Event(kind, "join " + joined, joined);
    }

    private Event leave(int count) {
        List<String> left = new ArrayList<>();
        for (int i = 0; i < count && !members.isEmpty(); i++) {
            List<String> ids = new ArrayList<>(members.keySet());
            String id = ids.get(random.nextInt(ids.size()));
            members.remove(id);
            left.add(id);
        }
        if (left.isEmpty()) {
            return new Event(Kind.LEAVE_MANY, "nobody leaves, the group is empty", left);
        }
        Kind kind = left.size() == 1 ? Kind.LEAVE : Kind.LEAVE_MANY;
        return new Event(kind, "leave " + left, left);
    }

    private Event growTopic() {
        List<Topic> candidates = new ArrayList<>();
        for (Topic topic : topics) {
            if (topic.partitions() < MAX_PARTITIONS) {
                candidates.add(topic);
            }
        }
        if (candidates.isEmpty()) {
            return new Event(Kind.GROW_TOPIC, "grow nothing", List.of());
        }
        Topic topic = candidates.get(random.nextInt(candidates.size()));
        int added = switch (size) {
            case TINY -> 1 + random.nextInt(2);
            case MEDIUM -> 1 + random.nextInt(10);
            case LARGE -> 1 + random.nextInt(20);
        };
        added = Math.min(added, MAX_PARTITIONS - topic.partitions());
        for (int i = 0; i < added; i++) {
            topic.replicaRacks.add(replicaRacks(topic, topic.partitions()));
        }
        return new Event(Kind.GROW_TOPIC, "grow " + topic.name + " by " + added + " to " + topic.partitions(), List.of());
    }

    private Event addTopicToCohort() {
        int cohort = random.nextInt(cohorts.size());
        Set<Uuid> subscription = cohorts.get(cohort);
        List<Uuid> candidates = sorted(allTopics());
        candidates.removeAll(subscription);
        Topic topic;
        if (topics.size() < MAX_TOPICS && (candidates.isEmpty() || random.nextInt(3) == 0)) {
            topic = createTopic(initialPartitions());
        } else if (candidates.isEmpty()) {
            return new Event(Kind.ADD_TOPIC_TO_COHORT, "cohort " + cohort + " adds nothing", List.of());
        } else {
            topic = topicsById.get(candidates.get(random.nextInt(candidates.size())));
        }
        subscription.add(topic.id);
        for (Member member : members.values()) {
            if (member.cohort == cohort) {
                member.subscription.add(topic.id);
            }
        }
        return new Event(Kind.ADD_TOPIC_TO_COHORT, "cohort " + cohort + " adds " + topic.name, List.of());
    }

    /**
     * Drops a topic from a cohort keeping at least one. Members of the cohort follow, unless
     * that would leave them without any topic.
     */
    private Event removeTopicFromCohort() {
        int cohort = random.nextInt(cohorts.size());
        Set<Uuid> subscription = cohorts.get(cohort);
        if (subscription.size() <= 1) {
            return new Event(Kind.REMOVE_TOPIC_FROM_COHORT, "cohort " + cohort + " drops nothing", List.of());
        }
        List<Uuid> candidates = sorted(subscription);
        Uuid topicId = candidates.get(random.nextInt(candidates.size()));
        subscription.remove(topicId);
        for (Member member : members.values()) {
            if (member.cohort == cohort && member.subscription.size() > 1) {
                member.subscription.remove(topicId);
            }
        }
        return new Event(Kind.REMOVE_TOPIC_FROM_COHORT, "cohort " + cohort + " drops " + topicsById.get(topicId).name, List.of());
    }

    /**
     * A member subscribes to a topic it does not have, or drops one it has, keeping at least one.
     */
    private Event toggleTopic() {
        if (members.isEmpty()) {
            return new Event(Kind.TOGGLE_TOPIC, "nobody toggles", List.of());
        }
        Member member = randomMember();
        Topic topic = topics.get(random.nextInt(topics.size()));
        if (!member.subscription.contains(topic.id)) {
            member.subscription.add(topic.id);
            return new Event(Kind.TOGGLE_TOPIC, member.id + " subscribes to " + topic.name, List.of());
        }
        if (member.subscription.size() == 1) {
            return new Event(Kind.TOGGLE_TOPIC, member.id + " keeps " + topic.name, List.of());
        }
        member.subscription.remove(topic.id);
        return new Event(Kind.TOGGLE_TOPIC, member.id + " drops " + topic.name, List.of());
    }

    private Event moveReplicas() {
        Topic topic = topics.get(random.nextInt(topics.size()));
        int moved = 0;
        for (int p = 0; p < topic.partitions(); p++) {
            if (random.nextInt(10) < 3) {
                topic.replicaRacks.set(p, randomRackSet());
                moved++;
            }
        }
        return new Event(Kind.MOVE_REPLICAS, "replicas of " + moved + " partitions of " + topic.name + " move", List.of());
    }

    /**
     * A member moves to another rack, possibly the broker-less one, or loses its rack unless
     * every member must have one.
     */
    private Event changeRack() {
        if (members.isEmpty()) {
            return new Event(Kind.CHANGE_RACK, "nobody changes rack", List.of());
        }
        Member member = randomMember();
        List<String> racks = new ArrayList<>(BROKER_RACKS);
        racks.add(BROKERLESS_RACK);
        if (rackMode != RackMode.ALL) {
            racks.add(null);
        }
        racks.remove(member.rack);
        String from = member.rack == null ? "-" : member.rack;
        member.rack = racks.get(random.nextInt(racks.size()));
        String to = member.rack == null ? "-" : member.rack;
        return new Event(Kind.CHANGE_RACK, member.id + " moves from rack " + from + " to " + to, List.of());
    }

    private Member randomMember() {
        List<String> ids = new ArrayList<>(members.keySet());
        return members.get(ids.get(random.nextInt(ids.size())));
    }

    // Helpers.

    @SafeVarargs
    private <T> T pick(T... choices) {
        return choices[random.nextInt(choices.length)];
    }

    private static List<Uuid> sorted(Set<Uuid> topicIds) {
        return new ArrayList<>(new TreeSet<>(topicIds));
    }

    /**
     * @return A copy of the partitions with the topics and the partitions in ascending order.
     */
    private static Map<Uuid, Set<Integer>> copy(Map<Uuid, Set<Integer>> partitions) {
        Map<Uuid, Set<Integer>> copy = new TreeMap<>();
        partitions.forEach((topicId, topicPartitions) -> copy.put(topicId, new TreeSet<>(topicPartitions)));
        return copy;
    }

    private List<String> topicNames(Set<Uuid> topicIds) {
        List<String> names = new ArrayList<>();
        for (Topic topic : topics) {
            if (topicIds.contains(topic.id)) {
                names.add(topic.name);
            }
        }
        return names;
    }

    private String currentPartitions(Map<Uuid, Set<Integer>> current) {
        StringBuilder out = new StringBuilder("{");
        for (Topic topic : topics) {
            Set<Integer> partitions = current.get(topic.id);
            if (partitions != null) {
                out.append(out.length() == 1 ? "" : ", ").append(topic.name).append('=').append(new TreeSet<>(partitions));
            }
        }
        return out.append('}').toString();
    }
}
