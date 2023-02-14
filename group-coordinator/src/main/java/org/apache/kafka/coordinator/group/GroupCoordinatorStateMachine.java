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
import org.slf4j.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;

public class GroupCoordinatorStateMachine {

    private final int partitionId;

    private final LogContext logContext;

    private final Logger log;

    private final SnapshotRegistry snapshotRegistry;

    private final LinkedHashMap<String, PartitionAssignor> assignors;

    // ConsumerGroup State
    // Target Assignment
    // Current Assignment
    // Rebalance Timeouts
    // Session Timeouts

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
    }

    public Result<ConsumerGroupHeartbeatResponseData> joinConsumerGroup(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        // TODO
        // 1. Validate request
        // 2. Update group state
        // 3. Maybe compute new target assignment
        //    This needs the updated group state from 2. How to do this?
        //    a) when creating the assignor's input we could just inject the
        //       the new member state based on the records.
        //    b) we could have a staged approach where each steps would update
        //       the state before the next step in called.
        // 4. Reconcile member assignment
        //    This needs the updated target assignment from 3.


        /*

        appendWriteEvent("heartbeat", partitionId,
            (statemachine, response) -> statemachine.heartbeat(req, response),
            (statemachine, response) -> statemachine.maybeComputeAssignment(),
            (statemachine, response) -> statemachine.reconcile(response)
        ).handle(
            // Convert errors.
        )

         */

        return new Result<>(
            Collections.emptyList(),
            new ConsumerGroupHeartbeatResponseData(),
            true
        );
    }

    public void replay(Record record) {
        // TODO
    }
}
