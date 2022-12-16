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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class ReplicatedGroupCoordinatorPartition {

    class Record {
        private final ApiMessageAndVersion key;
        private final ApiMessageAndVersion value;

        public Record(
            ApiMessageAndVersion key,
            ApiMessageAndVersion value
        ) {
            this.key = key;
            this.value = value;
        }

        public ApiMessageAndVersion key() {
            return this.key;
        }

        public ApiMessageAndVersion value() {
            return this.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Record record = (Record) o;
            return Objects.equals(key, record.key) && Objects.equals(value, record.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "Record(key=" + key + ", value=" + value + ")";
        }
    }

    class Result<T> {
        private final List<Record> records;
        private final T response;
        private final boolean isAtomic;

        public Result(
            List<Record> records,
            T response,
            boolean isAtomic
        ) {
            this.records = records;
            this.response = response;
            this.isAtomic = isAtomic;
        }

        public List<Record> records() {
            return records;
        }

        public T response() {
            return response;
        }

        public boolean isAtomic() {
            return isAtomic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result<?> result = (Result<?>) o;
            return isAtomic == result.isAtomic &&
                Objects.equals(records, result.records) &&
                Objects.equals(response, result.response);
        }

        @Override
        public int hashCode() {
            return Objects.hash(records, response, isAtomic);
        }

        @Override
        public String toString() {
            return "Result(records=" + records +
                ", response=" + response +
                ", isAtomic=" + isAtomic +
                ")";
        }
    }

    interface WriteOperation<T> {
        Result<T> generateRecordsAndResponse() throws Exception;
    }

    class WriteEvent<T> implements Event, DeferredEvent {

        private final String name;

        private final CompletableFuture<T> future;

        private final WriteOperation<T> op;

        private T response;

        private long writeOffset;

        public WriteEvent(
            String name,
            WriteOperation<T> op
        ) {
            this.name = name;
            this.op = op;
            this.future = new CompletableFuture<T>();
        }

        @Override
        public void run() throws Exception {
            // Check if coordinator is running.

            Result<T> result = op.generateRecordsAndResponse();

            if (result.records.isEmpty()) {
                OptionalLong highestPendingOffset = purgatory.highestPendingOffset();
                if (highestPendingOffset.isPresent()) {
                    response = result.response;
                    writeOffset = highestPendingOffset.getAsLong();
                } else {
                    response = result.response;
                    writeOffset = -1;
                    complete();
                }
            } else {
                result.records.forEach(ReplicatedGroupCoordinatorPartition.this::replay);
                // Write -> Need offset
                // writeOffset = write(records)
                // in case of any errors, rollback the change and fail the event.
            }

            if (!future.isDone()) {
                purgatory.add(writeOffset, this);
            }
        }

        @Override
        public void complete() {
            // TODO Check null.
            future.complete(response);
        }

        @Override
        public void completeExceptionally(Throwable throwable) {
            // TODO - We need to convert errors.
            future.completeExceptionally(throwable);
        }
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

    private final TimelineHashMap<String, ConsumerGroupState> groups;

    private final CoordinatorPurgatory purgatory;

    private final long lastCommittedOffset = -1L;
    private final long lastWriteOffset = -1L;

    // Purgatory? -> Park requests until they are committed.
    // Timer -> Session timeout.

    // Target Assignment?
    // Current Assignment?
    // Session Management?

    // partition epoch

    private final Queue<Event> queue = new LinkedBlockingQueue<>();

    private State state;

    public ReplicatedGroupCoordinatorPartition() {
        this.partitionIndex = 0;
        this.logContext = new LogContext(String.format("[GroupCoordinator %d] ", partitionIndex));
        this.log = logContext.logger(ReplicatedGroupCoordinatorPartition.class);
        this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.purgatory = new CoordinatorPurgatory();
    }

    public CompletableFuture<ConsumerGroupHeartbeatResponseData> joinConsumerGroup(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        return appendWriteEvent("joinConsumerGroup", () -> {
            // 0. Validate request
            // 0.1. Validate member id
            // 0.2. Validate member epoch
            // 1. Update group state
            // 2. Maybe update target assignment
            // 3. Reconcile
            String groupId = request.groupId();
            String memberId = request.memberId();
            int groupEpoch = -1;

            List<Record> records = new ArrayList<>();

            ConsumerGroupState group = groups.get(groupId);
            if (group == null) {
                // Create group and member.
                groupEpoch = 1;
                memberId = Uuid.randomUuid().toString();

                records.add(new Record(
                    new ApiMessageAndVersion(
                        new ConsumerGroupMemberMetadataKey()
                            .setGroupId(groupId)
                            .setMemberId(memberId),
                        (short) 5
                    ),
                    new ApiMessageAndVersion(
                        new ConsumerGroupMemberMetadataValue()
                            .setGroupEpoch(groupEpoch)
                            .setInstanceId(request.instanceId())
                            .setRackId(request.rackId())
                            .setClientId(context.clientId())
                            .setClientHost(context.clientAddress.toString())
                            .setAssignors(Collections.emptyList()),
                        (short) 0
                    )
                ));
                records.add(new Record(
                    new ApiMessageAndVersion(
                        new ConsumerGroupMetadataKey()
                            .setGroupId(groupId),
                        (short) 3
                    ),
                    new ApiMessageAndVersion(
                        new ConsumerGroupMetadataValue()
                            .setEpoch(groupEpoch),
                        (short) 0
                    )
                ));
            } else {
                // Update member if necessary.
                // TODO: Check if we need to update it.
                groupEpoch = group.groupEpoch() + 1;
                records.add(new Record(
                    new ApiMessageAndVersion(
                        new ConsumerGroupMemberMetadataKey()
                            .setGroupId(groupId)
                            .setMemberId(memberId),
                        (short) 5
                    ),
                    new ApiMessageAndVersion(
                        new ConsumerGroupMemberMetadataValue()
                            .setGroupEpoch(groupEpoch)
                            .setInstanceId(request.instanceId())
                            .setRackId(request.rackId())
                            .setClientId(context.clientId())
                            .setClientHost(context.clientAddress.toString())
                            .setAssignors(Collections.emptyList()),
                        (short) 0
                    )
                ));
                records.add(new Record(
                    new ApiMessageAndVersion(
                        new ConsumerGroupMetadataKey()
                            .setGroupId(groupId),
                        (short) 3
                    ),
                    new ApiMessageAndVersion(
                        new ConsumerGroupMetadataValue()
                            .setEpoch(groupEpoch),
                        (short) 0
                    )
                ));
            }

            return new Result<>(
                records,
                new ConsumerGroupHeartbeatResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setMemberId(memberId)
                    .setMemberEpoch(groupEpoch),
                true
            );
        });
    }

    public void poll() throws Exception {
        Event event = queue.poll();
        if (event != null) {
            try {
                event.run();
            } catch (Throwable throwable) {
                if (event instanceof DeferredEvent) {
                    ((DeferredEvent) event).completeExceptionally(throwable);
                }
            }
        }
    }

    // close.

    private <T> CompletableFuture<T> appendWriteEvent(
        String name,
        WriteOperation<T> op
    ) {
        WriteEvent<T> writeEvent = new WriteEvent<>(name, op);
        queue.add(writeEvent);
        return writeEvent.future;
    }

    private void replay(Record record) {
        switch (record.key.version()) {
            case 3:
                replayConsumerGroupMetadata(
                    (ConsumerGroupMetadataKey) record.key.message(),
                    (ConsumerGroupMetadataValue) record.value.message()
                );
                break;
            case 4:
                replayConsumerGroupPartitionMetadata(
                    (ConsumerGroupPartitionMetadataKey) record.key.message(),
                    (ConsumerGroupPartitionMetadataValue) record.value.message()
                );
                break;
            case 5:
                replayConsumerGroupMemberMetadata(
                    (ConsumerGroupMemberMetadataKey) record.key.message(),
                    (ConsumerGroupMemberMetadataValue) record.value.message()
                );
                break;
            default:
                throw new RuntimeException("Unhandled record type " + record.key.version());
        }
    }

    private void replayConsumerGroupMetadata(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value == null) {
            groups.remove(groupId);
        } else {
            ConsumerGroupState group = groups.get(groupId);
            if (group == null) {
                group = new ConsumerGroupState(
                    key.groupId(),
                    snapshotRegistry
                );
                groups.put(key.groupId(), group);
            }
            group.setGroupEpoch(value.epoch());
        }
    }

    private void replayConsumerGroupPartitionMetadata(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        // TODO
    }

    private void replayConsumerGroupMemberMetadata(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroupState group = groups.get(groupId);

        if (value == null) {
            if (group != null) {
                group.removeMember(memberId);
            }
        } else {
            if (group == null) {
                group = new ConsumerGroupState(
                    key.groupId(),
                    snapshotRegistry
                );
                groups.put(groupId, group);
            }

            MemberState member = group.member(memberId);
            if (member == null) {
                group.removeMember(memberId);
            } else {
                member = MemberState.fromRecord(key, value);
                group.putMember(member);
            }
        }
    }
}
