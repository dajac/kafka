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

import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentBuilder;

import java.util.Map;

/**
 * The Uniform2 assignor distributes the partitions of the subscribed topics among the
 * members of the group such that:
 * <ul>
 *     <li>the partitions of every topic are spread evenly across the members subscribed to
 *     it (the counts differ by at most one);</li>
 *     <li>the total number of partitions per member is balanced (within one for homogeneous
 *     subscriptions, and up to a local optimum for heterogeneous subscriptions);</li>
 *     <li>partitions stay on their current owner whenever the two properties above allow it
 *     (stickiness).</li>
 * </ul>
 *
 * The same algorithm is used for homogeneous and heterogeneous subscriptions.
 *
 * <p>The assignor is not registered as a built-in assignor yet: a coordinator enables it by
 * naming its class in {@code group.consumer.assignors}.
 *
 * @see AssignmentBuilder
 */
public class Uniform2Assignor implements ConsumerGroupPartitionAssignor {
    public static final String NAME = "uniform2";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty())
            return new GroupAssignment(Map.of());

        return new AssignmentBuilder(groupSpec, subscribedTopicDescriber).build();
    }
}
