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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.AssignorHelpers;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collects the partition sets of the members, one topic at a time, and builds the group
 * assignment. A member whose partitions did not change at all gets its current assignment
 * back, the very same instance, so that the caller can recognize unchanged members cheaply.
 */
final class AssignmentResult {
    private final GroupModel model;
    private final IntList entryMember;
    private final IntList entryTopic;
    private final List<Set<Integer>> entryPartitions;
    /** Per member, whether its assignment differs from its current one. */
    private final boolean[] memberChanged;

    AssignmentResult(GroupModel model) {
        this.model = model;
        // Most of the current entries are usually emitted unchanged, plus a few new ones.
        int expectedEntries = Math.max(1024, model.holderMember.length + model.memberCount);
        entryMember = new IntList(expectedEntries);
        entryTopic = new IntList(expectedEntries);
        entryPartitions = new ArrayList<>(expectedEntries);
        memberChanged = model.hasDroppedPartitions.clone();
    }

    /**
     * Records the partitions of a topic assigned to a member. The set is the member's current
     * one when its partitions of the topic did not change.
     */
    void add(int member, int topic, Set<Integer> partitions) {
        entryMember.add(member);
        entryTopic.add(topic);
        entryPartitions.add(partitions);
    }

    /**
     * Records that the assignment of the member differs from its current one.
     */
    void markChanged(int member) {
        memberChanged[member] = true;
    }

    GroupAssignment build() {
        int entryCount = entryMember.size();
        int[] entryStart = new int[model.memberCount + 1];
        for (int i = 0; i < entryCount; i++) {
            entryStart[entryMember.get(i) + 1]++;
        }
        for (int m = 0; m < model.memberCount; m++) {
            entryStart[m + 1] += entryStart[m];
        }
        int[] order = new int[entryCount];
        int[] fill = Arrays.copyOf(entryStart, model.memberCount);
        for (int i = 0; i < entryCount; i++) {
            order[fill[entryMember.get(i)]++] = i;
        }

        Map<String, MemberAssignment> members = AssignorHelpers.newHashMap(model.memberCount);
        for (int m = 0; m < model.memberCount; m++) {
            Map<Uuid, Set<Integer>> assignment;
            if (!memberChanged[m]) {
                assignment = model.currentAssignments[m];
            } else {
                assignment = AssignorHelpers.newHashMap(entryStart[m + 1] - entryStart[m]);
                for (int i = entryStart[m]; i < entryStart[m + 1]; i++) {
                    int entry = order[i];
                    assignment.put(model.topicIds[entryTopic.get(entry)], entryPartitions.get(entry));
                }
            }
            members.put(model.memberIds[m], new MemberAssignmentImpl(assignment));
        }
        return new GroupAssignment(members);
    }
}
