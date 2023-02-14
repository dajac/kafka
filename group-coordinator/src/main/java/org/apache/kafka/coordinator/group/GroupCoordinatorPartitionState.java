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


/*

{
    // Contains all the state related of an __consumer_offsets partitions
    PartitionId
    State: Loading, Loaded (Registering), Active, Unloading, Unloaded, Failed
    SnapshotRegistry
    Purgatory

    GroupMetadataManager
    SessionManager

    LastWrittenOffset
    LastCommittedOffset

    handleX() -> Response and Records
    replayX() -> Update in memory state
    commit(offset) -> Advance LastCommittedOffset, complete pending requests, etc.
}
 */


import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerPurgatory;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public class GroupCoordinatorPartitionState implements AutoCloseable {

    public enum State {
        LOADING,
        RUNNING,
        UNLOADING,
        FAILED,
        CLOSED
    }

    private final int partitionId;

    private final LogContext logContext;

    private final Logger log;

    private SnapshotRegistry snapshotRegistry;

    // TODO: Move this to sever-common.
    private ControllerPurgatory purgatory;

    private State state;

    private long epoch = -1L;

    private long lastWrittenOffset = -1L;

    private long lastCommittedOffset = -1L;

    // Group State? GroupMetadataManager or here directly?

    public GroupCoordinatorPartitionState(
        int partitionId,
        LogContext logContext
    ) {
        this.partitionId = partitionId;
        this.logContext = logContext;
        this.log = logContext.logger(GroupCoordinatorPartitionState.class);
        this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.purgatory = new ControllerPurgatory();
        this.state = State.RUNNING;
    }

    public CompletableFuture<ConsumerGroupHeartbeatResponseData> joinConsumerGroup(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        // Need to return the response and the records.
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    public void replay() {
        // Replay records.
    }

    public void commitUpTo(int offset) {
        // Commit state.
        // Release reponses.
        // Update lastCommitterOffset.
    }

    @Override
    public void close() throws Exception {

    }
}
