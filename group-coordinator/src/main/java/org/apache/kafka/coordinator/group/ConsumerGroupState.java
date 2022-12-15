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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of a consumer group.
 */
public class ConsumerGroupState {

    /**
     * The group id.
     */
    private final String groupId;

    /**
     * The group epoch. The group epoch is bumped whenever the group
     * is updated. This signals that a new assignment for the group
     * is required.
     */
    private final TimelineInteger groupEpoch;

    /**
     * The group members.
     */
    private final TimelineHashMap<String, MemberState> members;

    public ConsumerGroupState(
        String groupId,
        SnapshotRegistry snapshotRegistry
    ) {
        this.groupId = groupId;
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public String groupId() {
        return this.groupId;
    }

    public int groupEpoch() {
        return this.groupEpoch.get();
    }

    public Map<String, MemberState> members() {
        return new HashMap<>(this.members);
    }

    public void setGroupEpoch(int newGroupEpoch) {
        groupEpoch.set(newGroupEpoch);
    }

    public boolean putMember(MemberState newMember) {
        return members.put(newMember.memberId(), newMember) == null;
    }

    public boolean removeMember(String memberId) {
        return members.remove(memberId) != null;
    }

    public MemberState member(String memberId) {
        return members.get(memberId);
    }
}
