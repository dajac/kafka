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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConsumerGroupStateTest {

    @Test
    public void playground() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);

        TimelineHashMap<String, ConsumerGroupState> groups = new TimelineHashMap<>(snapshotRegistry, 0);

        snapshotRegistry.getOrCreateSnapshot(0);

        // Add group.
        ConsumerGroupState group = new ConsumerGroupState("foo", snapshotRegistry);
        groups.put("foo", group);

        assertEquals("foo", groups.get("foo").groupId());
        assertEquals(0, groups.get("foo").groupEpoch());
        assertEquals(Collections.emptyMap(), groups.get("foo").members());

        snapshotRegistry.getOrCreateSnapshot(1);

        // Add member.
        MemberState member = new MemberState(
            Uuid.randomUuid().toString(),
            Optional.empty(),
            Optional.empty(),
            "client-id",
            "client-host",
            Arrays.asList("foo", "bar"),
            "",
            Optional.empty(),
            Collections.emptyList()
        );

        group.putMember(member);

        assertEquals(member, group.member(member.memberId()));

        snapshotRegistry.getOrCreateSnapshot(2);

        // Update member.
        MemberState updatedMember = new MemberState(
            member.memberId(),
            Optional.of("instance-id"),
            Optional.empty(),
            "client-id",
            "client-host",
            Arrays.asList("foo", "bar", "zar"),
            "",
            Optional.empty(),
            Collections.emptyList()
        );

        group.putMember(updatedMember);

        assertEquals(updatedMember, group.member(member.memberId()));

        snapshotRegistry.revertToSnapshot(2);

        assertEquals(member, group.member(member.memberId()));

        snapshotRegistry.revertToSnapshot(1);
        assertNull(group.member(member.memberId()));

        snapshotRegistry.revertToSnapshot(0);
        assertNull(groups.get("foo"));
    }
}
