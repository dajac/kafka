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
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkOrderedAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CanaryAssignorTest {
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final String topic1Name = "topic1";
    private final String topic2Name = "topic2";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    private CanaryAssignor createAssignor(String regex, int maxPartitions) {
        CanaryAssignor assignor = new CanaryAssignor();
        assignor.configure(Map.of(
            CanaryAssignor.INSTANCE_ID_REGEX_CONFIG, regex,
            CanaryAssignor.MAX_PARTITIONS_PER_MEMBER_CONFIG, String.valueOf(maxPartitions)
        ));
        return assignor;
    }

    private CanaryAssignor createAssignor(String regex) {
        CanaryAssignor assignor = new CanaryAssignor();
        assignor.configure(Map.of(
            CanaryAssignor.INSTANCE_ID_REGEX_CONFIG, regex
        ));
        return assignor;
    }

    @Test
    public void testEmptyGroup() {
        CanaryAssignor assignor = createAssignor("canary-.*");

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            Map.of(),
            HOMOGENEOUS,
            Map.of()
        );

        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);
        assertEquals(Map.of(), result.members());
    }

    @Test
    public void testNoCanaryMembers() {
        // All members are non-canary. Partitions are distributed uniformly.
        CanaryAssignor assignor = createAssignor("canary-.*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // 6 partitions / 2 members = 3 each.
        int totalA = partitionCount(result, memberA);
        int totalB = partitionCount(result, memberB);
        assertEquals(3, totalA);
        assertEquals(3, totalB);
    }

    @Test
    public void testOneCanaryOneNonCanaryFirstAssignment() {
        // Canary member gets at most 2 partitions, non-canary gets the rest.
        CanaryAssignor assignor = createAssignor("canary-.*", 2);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        // Member A is canary (has instance id matching "canary-.*").
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        // Member B is non-canary.
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Canary gets 2, non-canary gets 4.
        assertEquals(2, partitionCount(result, memberA));
        assertEquals(4, partitionCount(result, memberB));
    }

    @Test
    public void testOneCanaryTwoNonCanaryFirstAssignment() {
        // 10 partitions, 1 canary with max 2, 2 non-canary members.
        // Canary gets 2, remaining 8 split into 4+4.
        CanaryAssignor assignor = createAssignor("canary-.*", 2);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 10)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(2, partitionCount(result, memberA));
        assertEquals(4, partitionCount(result, memberB));
        assertEquals(4, partitionCount(result, memberC));
    }

    @Test
    public void testTwoCanaryTwoNonCanaryFirstAssignment() {
        // 10 partitions, 2 canary with max 1, 2 non-canary members.
        // Canary gets 1 each (2 total), remaining 8 split into 4+4.
        CanaryAssignor assignor = createAssignor("canary-.*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 10)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-2"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put("D", new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(1, partitionCount(result, memberA));
        assertEquals(1, partitionCount(result, memberB));
        assertEquals(4, partitionCount(result, memberC));
        assertEquals(4, partitionCount(result, "D"));
    }

    @Test
    public void testNonCanaryOddDistribution() {
        // 7 partitions, 1 canary with max 1, 2 non-canary.
        // Canary gets 1, remaining 6 split into 3+3.
        CanaryAssignor assignor = createAssignor("canary-.*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 7)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(1, partitionCount(result, memberA));
        assertEquals(3, partitionCount(result, memberB));
        assertEquals(3, partitionCount(result, memberC));
    }

    @Test
    public void testNonCanaryUnevenDistribution() {
        // 8 partitions, 1 canary with max 1, 2 non-canary.
        // Canary gets 1, remaining 7 split into 4+3.
        CanaryAssignor assignor = createAssignor("canary-.*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 8)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(1, partitionCount(result, memberA));
        // One non-canary gets 4, the other 3.
        int totalB = partitionCount(result, memberB);
        int totalC = partitionCount(result, memberC);
        assertEquals(7, totalB + totalC);
        assertTrue(Math.abs(totalB - totalC) <= 1);
    }

    @Test
    public void testDefaultMaxPartitionsIsOne() {
        // Use the default max partitions (1).
        CanaryAssignor assignor = createAssignor("canary-.*");

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Default is 1 partition for canary.
        assertEquals(1, partitionCount(result, memberA));
        assertEquals(5, partitionCount(result, memberB));
    }

    @Test
    public void testStickinessCanaryMemberRetainsPartitions() {
        // Canary member already has 2 partitions assigned (within quota).
        // Non-canary member has 4 partitions. No reassignment needed.
        CanaryAssignor assignor = createAssignor("canary-.*", 2);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1)
            ))
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 2, 3, 4, 5)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Canary retains its 2 partitions. Non-canary retains its 4.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2, 3, 4, 5)
        ));
        assertAssignment(expectedAssignment, result);
    }

    @Test
    public void testCanaryMemberRevokesExcessPartitions() {
        // Canary member has 4 partitions but max is 2. Two should be revoked
        // and given to the non-canary member.
        CanaryAssignor assignor = createAssignor("canary-.*", 2);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2, 3)
            ))
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 4, 5)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Canary should have 2, non-canary should have 4.
        assertEquals(2, partitionCount(result, memberA));
        assertEquals(4, partitionCount(result, memberB));
    }

    @Test
    public void testCanaryMemberWithZeroMaxPartitions() {
        // Canary members with max 0 partitions get nothing.
        CanaryAssignor assignor = createAssignor("canary-.*", 0);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(0, partitionCount(result, memberA));
        assertEquals(6, partitionCount(result, memberB));
    }

    @Test
    public void testMultipleTopics() {
        // 2 topics (3 + 3 = 6 partitions), 1 canary with max 1, 1 non-canary.
        CanaryAssignor assignor = createAssignor("canary-.*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .addTopic(topic2Uuid, topic2Name, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        assertEquals(1, partitionCount(result, memberA));
        assertEquals(5, partitionCount(result, memberB));
    }

    @Test
    public void testNewNonCanaryMemberJoins() {
        // Existing: canary A (2 partitions), non-canary B (4 partitions).
        // New non-canary C joins. Canary keeps 2, B and C share the remaining 4.
        CanaryAssignor assignor = createAssignor("canary-.*", 2);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1)
            ))
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 2, 3, 4, 5)
            ))
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Canary keeps 2. Non-canary share 4: 2 each.
        assertEquals(2, partitionCount(result, memberA));
        assertEquals(2, partitionCount(result, memberB));
        assertEquals(2, partitionCount(result, memberC));
    }

    @Test
    public void testRegexMatchesPartialInstanceId() {
        // Regex ".*-canary" matches "prod-canary" but not "canary-prod".
        CanaryAssignor assignor = createAssignor(".*-canary", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("prod-canary"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-prod"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // A matches (canary), B does not match (non-canary).
        assertEquals(1, partitionCount(result, memberA));
        assertEquals(5, partitionCount(result, memberB));
    }

    @Test
    public void testMembersWithoutInstanceIdAreNonCanary() {
        // Members without instance id never match the canary regex.
        CanaryAssignor assignor = createAssignor(".*", 1);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        // No instance id.
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // Both are non-canary, so uniform distribution.
        assertEquals(3, partitionCount(result, memberA));
        assertEquals(3, partitionCount(result, memberB));
    }

    @Test
    public void testAllCanaryMembersDistributeAllPartitions() {
        // All members are canary with max 3 each, but only 4 total partitions.
        // They should still distribute all partitions.
        CanaryAssignor assignor = createAssignor("canary-.*", 3);

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 4)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-1"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("canary-2"),
            Set.of(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(members, HOMOGENEOUS, Map.of());
        GroupAssignment result = assignor.assign(groupSpec, subscribedTopicMetadata);

        // 4 partitions / 2 canary members = 2 each (within the max of 3).
        assertEquals(2, partitionCount(result, memberA));
        assertEquals(2, partitionCount(result, memberB));
    }

    private int partitionCount(GroupAssignment assignment, String memberId) {
        MemberAssignment memberAssignment = assignment.members().get(memberId);
        if (memberAssignment == null) return 0;
        return memberAssignment.partitions().values().stream()
            .mapToInt(Set::size)
            .sum();
    }
}
