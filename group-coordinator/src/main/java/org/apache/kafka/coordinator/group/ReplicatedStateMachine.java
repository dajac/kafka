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
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class ReplicatedStateMachine {

    public interface Event {
        void run();
    }

    public enum State {
        LOADING,
        ACTIVE,
        FAILED,
        UNLOADING,
        CLOSED
    }

    private final int partitionIndex;

    private final LogContext logContext;

    private final Logger log;

    private final SnapshotRegistry snapshotRegistry;

    private final TimelineHashMap<String, ConsumerGroup> groups;

    // Target Assignment?
    // Current Assignment?
    // Session Management?

    private final Queue<Event> queue = new LinkedBlockingQueue<>();

    private State state;

    public ReplicatedStateMachine() {
        this.partitionIndex = 0;
        this.logContext = new LogContext(String.format("[GroupCoordinator %d] ", partitionIndex));
        this.log = logContext.logger(ReplicatedStateMachine.class);
        this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public CompletableFuture<ConsumerGroupHeartbeatResponseData> joinConsumerGroup(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        // Validate the request
        // Put the request to the queue

        // Steps:
        // 1. Update the group state
        // 2. Update the target assignment (this needs 1's updated state)
        // 3. Reconcile (this needs 2's updated state)

//        queue.add(___ -> {
//
//        });

        return null;
    }

    public void poll() {
        // Execute one step.
        // Poll from the queue and execute the task.
    }

    public void replay() {
        // Should the replay add an event to the queue as well?
    }

    // close.
}
