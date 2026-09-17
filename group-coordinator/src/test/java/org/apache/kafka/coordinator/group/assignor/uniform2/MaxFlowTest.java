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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaxFlowTest {

    @Test
    public void testSingleGroupAndRack() {
        // Capped by the demand.
        assertFlow(new int[][] {{2}}, MaxFlow.compute(new long[] {0b1}, new int[] {3}, new int[] {2}));
        // Capped by the supply.
        assertFlow(new int[][] {{3}}, MaxFlow.compute(new long[] {0b1}, new int[] {3}, new int[] {5}));
        // Exact.
        assertFlow(new int[][] {{3}}, MaxFlow.compute(new long[] {0b1}, new int[] {3}, new int[] {3}));
    }

    @Test
    public void testGroupOnlyFlowsToTheRacksOfItsMask() {
        // Rack 1 only, although rack 0 has demand too.
        assertFlow(new int[][] {{0, 2}}, MaxFlow.compute(new long[] {0b10}, new int[] {2}, new int[] {2, 2}));
        // Racks 0 and 2, not rack 1 which has the largest demand.
        assertFlow(new int[][] {{1, 0, 1}}, MaxFlow.compute(new long[] {0b101}, new int[] {2}, new int[] {1, 5, 1}));
        // No rack at all.
        assertFlow(new int[][] {{0, 0}}, MaxFlow.compute(new long[] {0b0}, new int[] {3}, new int[] {2, 2}));
    }

    @Test
    public void testFlowIsCappedByTheSupply() {
        // Two partitions for a demand of six: rack 0 takes at most one, so the other goes to rack 1.
        assertFlow(new int[][] {{1, 1}}, MaxFlow.compute(new long[] {0b11}, new int[] {2}, new int[] {1, 5}));
    }

    @Test
    public void testFlowIsCappedByTheDemand() {
        assertFlow(new int[][] {{1, 2}}, MaxFlow.compute(new long[] {0b11}, new int[] {10}, new int[] {1, 2}));
    }

    @Test
    public void testZeroDemand() {
        assertFlow(new int[][] {{0, 0}}, MaxFlow.compute(new long[] {0b11}, new int[] {3}, new int[] {0, 0}));
    }

    @Test
    public void testZeroSupply() {
        assertFlow(new int[][] {{0}, {0}}, MaxFlow.compute(new long[] {0b1, 0b1}, new int[] {0, 0}, new int[] {3}));
    }

    @Test
    public void testNoGroups() {
        int[][] flow = MaxFlow.compute(new long[0], new int[0], new int[] {1, 2});
        assertEquals(0, flow.length);
    }

    @Test
    public void testNoRacks() {
        int[][] flow = MaxFlow.compute(new long[] {0b0}, new int[] {2}, new int[0]);
        assertEquals(1, flow.length);
        assertEquals(0, flow[0].length);
    }

    @Test
    public void testGreedyChoiceWouldGetStuck() {
        // Group 0 can serve racks 0 and 1, group 1 only rack 0. Handing group 0 to rack 0 first
        // would leave group 1 without a rack; the maximum flow satisfies both demands.
        assertFlow(
            new int[][] {{0, 1}, {1, 0}},
            MaxFlow.compute(new long[] {0b11, 0b01}, new int[] {1, 1}, new int[] {1, 1})
        );
    }

    @Test
    public void testAugmentingPathThroughSeveralGroups() {
        // Group 0 can serve racks 0 and 1, group 1 racks 1 and 2 and group 2 only rack 0: the
        // only way to serve every rack is 0 to rack 1, 1 to rack 2 and 2 to rack 0.
        assertFlow(
            new int[][] {{0, 1, 0}, {0, 0, 1}, {1, 0, 0}},
            MaxFlow.compute(new long[] {0b011, 0b110, 0b001}, new int[] {1, 1, 1}, new int[] {1, 1, 1})
        );
    }

    @Test
    public void testTwoReplicasOverThreeRacksWithBalancedDemand() {
        // Partition i has replicas in racks i % 3 and (i + 1) % 3: with six partitions there
        // are two per replica rack set. Every rack needs two partitions, and all six can be
        // aligned.
        long[] groupRacks = {0b011, 0b110, 0b101};
        int[] supply = {2, 2, 2};
        int[] demand = {2, 2, 2};
        int[][] flow = MaxFlow.compute(groupRacks, supply, demand);
        assertValidFlow(groupRacks, supply, demand, flow);
        assertEquals(6, totalFlow(flow));
        for (int rack = 0; rack < demand.length; rack++) {
            assertEquals(demand[rack], rackFlow(flow, rack), "rack " + rack);
        }
    }

    @Test
    public void testTwoReplicasOverThreeRacksWithUnbalancedDemand() {
        // Rack 2 needs nothing: the {1, 2} partitions must go to rack 1 and the {0, 2}
        // partitions to rack 0, which leaves one {0, 1} partition for each of them.
        assertFlow(
            new int[][] {{1, 1, 0}, {0, 2, 0}, {2, 0, 0}},
            MaxFlow.compute(new long[] {0b011, 0b110, 0b101}, new int[] {2, 2, 2}, new int[] {3, 3, 0})
        );
    }

    @Test
    public void testPartialAlignmentWhenTheDemandCannotBeReached() {
        // Racks 0 and 1 need three partitions each but only two can reach rack 0 and one rack 1,
        // while the five partitions of rack 2 are not needed.
        assertFlow(
            new int[][] {{2, 0, 0}, {0, 1, 0}, {0, 0, 0}},
            MaxFlow.compute(new long[] {0b001, 0b010, 0b100}, new int[] {2, 1, 5}, new int[] {3, 3, 0})
        );
    }

    private static void assertFlow(int[][] expected, int[][] actual) {
        assertEquals(expected.length, actual.length, "number of groups");
        for (int group = 0; group < expected.length; group++) {
            assertArrayEquals(expected[group], actual[group], "group " + group);
        }
    }

    /**
     * Checks that the flow stays within the masks, the supplies and the demands.
     */
    private static void assertValidFlow(long[] groupRacks, int[] supply, int[] demand, int[][] flow) {
        assertEquals(groupRacks.length, flow.length);
        for (int group = 0; group < flow.length; group++) {
            assertEquals(demand.length, flow[group].length);
            int groupFlow = 0;
            for (int rack = 0; rack < demand.length; rack++) {
                assertTrue(flow[group][rack] >= 0, "negative flow for group " + group + " and rack " + rack);
                if (flow[group][rack] > 0) {
                    long rackBit = 1L << rack;
                    assertTrue((groupRacks[group] & rackBit) != 0,
                        "group " + group + " flows to rack " + rack + " which is not in its mask");
                }
                groupFlow += flow[group][rack];
            }
            assertTrue(groupFlow <= supply[group], "group " + group + " exceeds its supply");
        }
        for (int rack = 0; rack < demand.length; rack++) {
            assertTrue(rackFlow(flow, rack) <= demand[rack], "rack " + rack + " exceeds its demand");
        }
    }

    private static int rackFlow(int[][] flow, int rack) {
        int sum = 0;
        for (int[] groupFlow : flow) {
            sum += groupFlow[rack];
        }
        return sum;
    }

    private static int totalFlow(int[][] flow) {
        int sum = 0;
        for (int[] groupFlow : flow) {
            for (int value : groupFlow) {
                sum += value;
            }
        }
        return sum;
    }
}
