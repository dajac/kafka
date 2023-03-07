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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

public class GroupCoordinatorStateMachine {

    private final int partitionId;

    private final LogContext logContext;

    private final Logger log;

    private final SnapshotRegistry snapshotRegistry;

    private final LinkedHashMap<String, PartitionAssignor> assignors;

    private final TimelineHashMap<String, ConsumerGroup> groups;

    public GroupCoordinatorStateMachine(
        int partitionId,
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        LinkedHashMap<String, PartitionAssignor> assignors
    ) {
        this.partitionId = partitionId;
        this.logContext = logContext;
        this.log = logContext.logger(GroupCoordinatorStateMachine.class);
        this.snapshotRegistry = snapshotRegistry;
        this.assignors = assignors;
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public IncrementalResult<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        return new IncrementalResult<>(
            new ConsumerGroupHeartbeatResponseData(),
            Arrays.asList(
                (response) -> consumerGroupHeartbeat(
                    context,
                    request,
                    response
                ),
                (response) -> maybeComputeAssignment(
                    context,
                    request,
                    response
                ),
                (response) -> maybeReconcileAssignment(
                    context,
                    request,
                    response
                )
            )
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request,
        ConsumerGroupHeartbeatResponseData response
    ) {
        // 1. Validate request
        // 2. Update group state
        return new Result<>(
            Collections.emptyList(),
            response
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> maybeComputeAssignment(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request,
        ConsumerGroupHeartbeatResponseData response
    ) {
        // 3. Maybe compute new target assignment
        return new Result<>(
            Collections.emptyList(),
            response
        );
    }

    private Result<ConsumerGroupHeartbeatResponseData> maybeReconcileAssignment(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request,
        ConsumerGroupHeartbeatResponseData response
    ) {
        // 4. Reconcile member assignment
        return new Result<>(
            Collections.emptyList(),
            response
        );
    }

    public void replay(Record record) {
        // TODO
    }
}
