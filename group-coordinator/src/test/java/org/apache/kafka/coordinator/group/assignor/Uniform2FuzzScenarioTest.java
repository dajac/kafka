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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Event;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Kind;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.RackMode;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Size;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.spec;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.subscriptionType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the scenario generator of the fuzzer: a seed determines the scenario, the events keep
 * it consistent, and a few seeds cover every size, rack mode and kind of event.
 */
public class Uniform2FuzzScenarioTest {
    private static final int SEEDS = 100;
    private static final int EVENTS = 20;

    @Test
    public void testSeedDeterminesTheScenario() {
        for (long seed = 0; seed < SEEDS; seed++) {
            Uniform2FuzzScenario first = new Uniform2FuzzScenario(seed);
            Uniform2FuzzScenario second = new Uniform2FuzzScenario(seed);
            assertEquals(first.dump(), second.dump());
            for (int i = 0; i < EVENTS; i++) {
                assertEquals(first.mutate(), second.mutate(), "seed " + seed);
                assertEquals(first.dump(), second.dump(), "seed " + seed);
            }
        }
    }

    @Test
    public void testEventsKeepTheScenarioConsistent() {
        Set<Kind> kinds = EnumSet.noneOf(Kind.class);
        Set<Size> sizes = EnumSet.noneOf(Size.class);
        Set<RackMode> rackModes = EnumSet.noneOf(RackMode.class);
        boolean homogeneous = false;
        boolean heterogeneous = false;
        for (long seed = 0; seed < SEEDS; seed++) {
            Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(seed);
            sizes.add(scenario.size());
            rackModes.add(scenario.rackMode());
            assertConsistent(scenario, "seed " + seed + " initial");
            for (int i = 0; i < EVENTS; i++) {
                Event event = scenario.mutate();
                kinds.add(event.kind());
                assertConsistent(scenario, "seed " + seed + " after " + event);
                boolean single = subscriptionType(scenario.members()) == SubscriptionType.HOMOGENEOUS;
                homogeneous |= single && scenario.memberCount() > 1;
                heterogeneous |= !single;
            }
        }
        assertEquals(EnumSet.complementOf(EnumSet.of(Kind.INIT)), kinds, "every kind of event happens");
        assertEquals(EnumSet.allOf(Size.class), sizes, "every size is generated");
        assertEquals(EnumSet.allOf(RackMode.class), rackModes, "every rack mode is generated");
        assertTrue(homogeneous && heterogeneous, "both subscription types are generated");
    }

    @Test
    public void testApplyReplacesTheCurrentPartitions() {
        Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(7);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = scenario.members();
        assertTrue(members.values().stream().allMatch(member -> member.partitions().isEmpty()), "nothing is held at first");

        GroupAssignment result = new Uniform2Assignor().assign(spec(members), scenario.describer());
        scenario.apply(result);

        Map<String, MemberSubscriptionAndAssignmentImpl> applied = scenario.members();
        assertEquals(members.keySet(), applied.keySet());
        applied.forEach((id, member) -> assertEquals(result.members().get(id).partitions(), member.partitions(), id));
        assertTrue(scenario.dump().contains("current={T"), "the dump shows the current partitions");
    }

    private static void assertConsistent(Uniform2FuzzScenario scenario, String context) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = scenario.members();
        SubscribedTopicDescriber describer = scenario.describer();
        assertEquals(scenario.memberCount(), members.size(), context);
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : members.entrySet()) {
            MemberSubscriptionAndAssignmentImpl member = entry.getValue();
            String memberContext = context + " member " + entry.getKey();
            assertFalse(member.subscribedTopicIds().isEmpty(), memberContext + " has no topic");
            member.rackId().ifPresent(rack -> assertTrue(
                Uniform2FuzzScenario.BROKER_RACKS.contains(rack) || Uniform2FuzzScenario.BROKERLESS_RACK.equals(rack),
                memberContext + " has an unknown rack " + rack));
            for (Uuid topicId : member.subscribedTopicIds()) {
                assertConsistentTopic(describer, topicId, memberContext);
            }
            member.partitions().forEach((topicId, partitions) -> {
                assertConsistentTopic(describer, topicId, memberContext);
                partitions.forEach(partition -> assertTrue(partition >= 0 && partition < describer.numPartitions(topicId),
                    memberContext + " holds the unknown partition " + topicId + "-" + partition));
            });
        }
    }

    private static void assertConsistentTopic(SubscribedTopicDescriber describer, Uuid topicId, String context) {
        int partitions = describer.numPartitions(topicId);
        assertTrue(partitions >= 1, context + ": topic " + topicId + " has " + partitions + " partitions");
        for (int partition = 0; partition < partitions; partition++) {
            Set<String> racks = describer.racksForPartition(topicId, partition);
            assertTrue(racks.size() >= 1 && racks.size() <= 3, context + ": " + topicId + "-" + partition + " has racks " + racks);
            assertTrue(Uniform2FuzzScenario.BROKER_RACKS.containsAll(racks), context + ": " + topicId + "-" + partition + " has racks " + racks);
        }
    }
}
