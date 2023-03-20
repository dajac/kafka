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

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ConsumerGroupMemberSubscriptionTest {

    @Test
    public void testConstructor() {
        ConsumerGroupMemberSubscription subscription = new ConsumerGroupMemberSubscription(
            "instance-id",
            "rack-id",
            5000,
            "client-id",
            "client-host",
            Arrays.asList("foo", "zar", "bar"),
            "regex-subscription",
            "assignor-name",
            Collections.singletonList(new AssignorState(
                "assignor",
                (byte) 0,
                (byte) 1,
                (byte) 2,
                (byte) 2,
                ByteBuffer.wrap("hello".getBytes())
            ))
        );

        assertEquals("instance-id", subscription.instanceId());
        assertEquals("rack-id", subscription.rackId());
        assertEquals(5000, subscription.rebalanceTimeoutMs());
        assertEquals("client-id", subscription.clientId());
        assertEquals("client-host", subscription.clientHost());
        // Topics are sorted.
        assertEquals(Arrays.asList("bar", "foo", "zar"), subscription.subscribedTopicNames());
        assertEquals("regex-subscription", subscription.subscribedTopicRegex());
        assertEquals("assignor-name", subscription.serverAssignorName());
        assertEquals(Collections.singletonList(new AssignorState(
            "assignor",
            (byte) 0,
            (byte) 1,
            (byte) 2,
            (byte) 2,
            ByteBuffer.wrap("hello".getBytes())
        )), subscription.assignorStates());
    }

    @Test
    public void testMaybeUpdateWith() {
        ConsumerGroupMemberSubscription subscription1 = new ConsumerGroupMemberSubscription(
            "instance-id",
            "rack-id",
            5000,
            "client-id",
            "client-host",
            Arrays.asList("foo", "zar", "bar"),
            "regex-subscription",
            "assignor-name",
            Collections.singletonList(new AssignorState(
                "assignor",
                (byte) 0,
                (byte) 1,
                (byte) 2,
                (byte) 2,
                ByteBuffer.wrap("hello".getBytes())
            ))
        );

        ConsumerGroupMemberSubscription subscription2 = subscription1.maybeUpdateWith(
            null,
            null,
            5000,
            "client-id",
            "client-host",
            null,
            null,
            null,
            null
        );

        assertSame(subscription1, subscription2);

        subscription2 = subscription1.maybeUpdateWith(
            "new-instance-id",
            "new-rack-id",
            5000,
            "client-id",
            "client-host",
            Collections.singletonList("foo"),
            "new-regex",
            "new-assignor",
            Collections.singletonList(new AssignorState(
                "new-assignor",
                (byte) 0,
                (byte) 1,
                (byte) 2,
                (byte) 2,
                ByteBuffer.wrap("hello".getBytes())
            ))
        );

        assertNotSame(subscription1, subscription2);
        assertNotEquals(subscription1, subscription2);

        assertEquals("new-instance-id", subscription2.instanceId());
        assertEquals("new-rack-id", subscription2.rackId());
        assertEquals(5000, subscription2.rebalanceTimeoutMs());
        assertEquals("client-id", subscription2.clientId());
        assertEquals("client-host", subscription2.clientHost());
        // Topics are sorted.
        assertEquals(Collections.singletonList("foo"), subscription2.subscribedTopicNames());
        assertEquals("new-regex", subscription2.subscribedTopicRegex());
        assertEquals("new-assignor", subscription2.serverAssignorName());
        assertEquals(Collections.singletonList(new AssignorState(
            "new-assignor",
            (byte) 0,
            (byte) 1,
            (byte) 2,
            (byte) 2,
            ByteBuffer.wrap("hello".getBytes())
        )), subscription2.assignorStates());
    }

    @Test
    public void testFromRecord() {
        ConsumerGroupMemberMetadataValue record = new ConsumerGroupMemberMetadataValue()
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Arrays.asList("zar", "foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignor("assignor")
            .setAssignors(Collections.singletonList(new ConsumerGroupMemberMetadataValue.Assignor()
                .setName("assignor")
                .setMinimumVersion((short) 1)
                .setMaximumVersion((short) 10)
                .setVersion((short) 5)
                .setMetadata("hello".getBytes())));

        ConsumerGroupMemberSubscription subscription = ConsumerGroupMemberSubscription.fromRecord(record);

        assertEquals(
            new ConsumerGroupMemberSubscription(
                "instance-id",
                "rack-id",
                5000,
                "client-id",
                "client-host",
                Arrays.asList("foo", "zar", "bar"),
                "regex",
                "assignor",
                Collections.singletonList(new AssignorState(
                    "assignor",
                    (byte) 0,
                    (byte) 1,
                    (byte) 10,
                    (byte) 5,
                    ByteBuffer.wrap("hello".getBytes())
                ))),
            subscription
        );
    }
}
