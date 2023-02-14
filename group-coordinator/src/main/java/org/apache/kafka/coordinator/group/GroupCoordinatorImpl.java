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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * New Group Coordinator.
 */
public class GroupCoordinatorImpl implements GroupCoordinator {

    public static class Builder {
        private Time time = Time.SYSTEM;
        private int numThreads = 1;
        private int consumerGroupSessionTimeoutMs = 45000;
        private int consumerGroupHeartbeatIntervalMs = 5000;
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private List<PartitionAssignor> consumerGroupAssignors = null;
        private int offsetsTopicSegmentBytes = 100 * 1024 * 1024;
        private Function<Integer, GroupCoordinatorEventLoop.Builder> createGroupCoordinatorEventLoopBuilder =
            GroupCoordinatorEventLoop.Builder::new;

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setNumThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder setConsumerGroupSessionTimeoutMs(int consumerGroupSessionTimeoutMs) {
            this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
            return this;
        }

        public Builder setConsumerGroupHeartbeatIntervalMs(int consumerGroupHeartbeatIntervalMs) {
            this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
            return this;
        }

        public Builder setConsumerGroupMaxSize(int consumerGroupMaxSize) {
            this.consumerGroupMaxSize = consumerGroupMaxSize;
            return this;
        }

        public Builder setConsumerGroupAssignors(List<PartitionAssignor> consumerGroupAssignors) {
            this.consumerGroupAssignors = consumerGroupAssignors;
            return this;
        }

        public Builder setCreateGroupCoordinatorEventLoopBuilder(Function<Integer, GroupCoordinatorEventLoop.Builder> createGroupCoordinatorEventLoopBuilder) {
            this.createGroupCoordinatorEventLoopBuilder = createGroupCoordinatorEventLoopBuilder;
            return this;
        }

        public Builder setOffsetsTopicSegmentBytes(int offsetsTopicSegmentBytes) {
            this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
            return this;
        }

        public GroupCoordinator build() {
            if (numThreads <= 0) {
                throw new IllegalArgumentException("The number of threads must be greater than 0.");
            }
            if (consumerGroupSessionTimeoutMs <= 0) {
                throw new IllegalArgumentException("Consumer group session timeout must be greater than 0.");
            }
            if (consumerGroupHeartbeatIntervalMs <= 0) {
                throw new IllegalArgumentException("Consumer group heartbeat interval must be greater than 0.");
            }
            if (consumerGroupMaxSize <= 0) {
                throw new IllegalArgumentException("Consumer group max size must be greater than 0.");
            }
            if (consumerGroupAssignors == null || consumerGroupAssignors.isEmpty()) {
                throw new IllegalArgumentException("At least one consumer group assignor must be specified.");
            }
            if (offsetsTopicSegmentBytes <= 0) {
                throw new IllegalArgumentException("Offsets topic segment bytes must be greater than 0.");
            }
            if (createGroupCoordinatorEventLoopBuilder == null) {
                throw new IllegalArgumentException("Event loop builder can not be null.");
            }

            return new GroupCoordinatorImpl(
                time,
                numThreads,
                consumerGroupSessionTimeoutMs,
                consumerGroupHeartbeatIntervalMs,
                consumerGroupMaxSize,
                Collections.unmodifiableList(consumerGroupAssignors),
                offsetsTopicSegmentBytes,
                createGroupCoordinatorEventLoopBuilder
            );
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinatorImpl.class);

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * The number of threads or event loops running.
     */
    private final int numThreads;

    /**
     * The consumer group session timeout in milliseconds.
     */
    private final int consumerGroupSessionTimeoutMs;

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    private final int consumerGroupHeartbeatIntervalMs;

    /**
     * The consumer group maximum size.
     */
    private final int consumerGroupMaxSize;

    /**
     * The consumer group assignors.
     */
    private final List<PartitionAssignor> consumerGroupAssignors;

    /**
     * The offsets topic segment bytes should be kept relatively small to facilitate faster
     * log compaction and faster offset loads.
     */
    private final int offsetsTopicSegmentBytes;

    /**
     * Function to create a new GroupCoordinatorEventLoop.Builder.
     */
    private final Function<Integer, GroupCoordinatorEventLoop.Builder> createGroupCoordinatorEventLoopBuilder;

    /**
     * This class holds the state of the group coordinator. It does not include the
     * group states but only the state relevant to dispatch requests to the right
     * event loop(s).
     */
    private class State {
        /**
         * The number of partitions of the __consumer_offsets topics.
         */
        final int groupMetadataTopicPartitionCount;

        /**
         * The list of event loops.
         */
        final List<GroupCoordinatorEventLoop> eventLoops;

        private State(
            int groupMetadataTopicPartitionCount,
            List<GroupCoordinatorEventLoop> eventLoops
        ) {
            this.groupMetadataTopicPartitionCount = groupMetadataTopicPartitionCount;
            this.eventLoops = eventLoops;
        }

        /**
         * Returns the partition id (or index) for the given group id.
         */
        int partitionId(String groupId) {
            return Utils.abs(groupId.hashCode()) % groupMetadataTopicPartitionCount;
        }

        /**
         * Returns the event loop id for the given partition id.
         */
        int eventLoopId(int partitionId) {
            return partitionId % numThreads;
        }

        /**
         * Returns the event loop for the given partition id.
         */
        GroupCoordinatorEventLoop eventLoopForPartitionId(int partitionId) {
            return eventLoops.get(eventLoopId(partitionId));
        }

        /**
         * Returns the event loop for the given group id.
         */
        GroupCoordinatorEventLoop eventLoopForGroupId(String groupId) {
            return eventLoopForPartitionId(partitionId(groupId));
        }
    }

    /**
     * The current state if the coordinator is running or null if not started or shut down.
     */
    private volatile State state = null;

    GroupCoordinatorImpl(
        Time time,
        int numThreads,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMaxSize,
        List<PartitionAssignor> consumerGroupAssignors,
        int offsetsTopicSegmentBytes,
        Function<Integer, GroupCoordinatorEventLoop.Builder> createGroupCoordinatorEventLoopBuilder
    ) {
        this.time = time;
        this.numThreads = numThreads;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupAssignors = consumerGroupAssignors;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
        this.createGroupCoordinatorEventLoopBuilder = createGroupCoordinatorEventLoopBuilder;
    }

    @Override
    public int partitionFor(
        String groupId
    ) {
        State localState = state;
        if (localState != null) {
            return localState.partitionId(groupId);
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        State localState = state;
        if (localState != null) {
            int partitionId = localState.partitionId(request.groupId());
            return localState.eventLoopForPartitionId(partitionId).joinConsumerGroup(
                partitionId,
                context,
                request
            );
        } else {
            return FutureUtils.failedFuture(Errors.NOT_COORDINATOR.exception());
        }
    }

    @Override
    public CompletableFuture<JoinGroupResponseData> joinGroup(
        RequestContext context,
        JoinGroupRequestData request,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<SyncGroupResponseData> syncGroup(
        RequestContext context,
        SyncGroupRequestData request,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<HeartbeatResponseData> heartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<LeaveGroupResponseData> leaveGroup(
        RequestContext context,
        LeaveGroupRequestData request
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<ListGroupsResponseData> listGroups(
        RequestContext context,
        ListGroupsRequestData request
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        RequestContext context,
        List<String> groupIds
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        RequestContext context,
        List<String> groupIds,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<List<OffsetFetchResponseData.OffsetFetchResponseTopics>> fetchOffsets(
        RequestContext context,
        String groupId,
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
        boolean requireStable
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<List<OffsetFetchResponseData.OffsetFetchResponseTopics>> fetchAllOffsets(
        RequestContext context,
        String groupId,
        boolean requireStable
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<OffsetCommitResponseData> commitOffsets(
        RequestContext context,
        OffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        RequestContext context,
        TxnOffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        RequestContext context,
        OffsetDeleteRequestData request,
        BufferSupplier bufferSupplier
    ) {
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
    }

    @Override
    public void onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    ) {
        State localState = state;
        if (localState != null) {
            localState.eventLoops.forEach(eventLoop ->
                eventLoop.onTransactionCompleted(producerId, partitions, transactionResult)
            );
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    ) {
        State localState = state;
        if (localState != null) {
            localState.eventLoops.forEach(eventLoop ->
                eventLoop.onPartitionsDeleted(topicPartitions, bufferSupplier)
            );
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        State localState = state;
        if (localState != null) {
            localState.eventLoops.forEach(eventLoop ->
                eventLoop.onNewMetadataImage(newImage, delta)
            );
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    ) {
        State localState = state;
        if (localState != null) {
            localState.eventLoopForPartitionId(groupMetadataPartitionIndex).onElection(
                groupMetadataPartitionIndex,
                groupMetadataPartitionLeaderEpoch
            );
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {
        State localState = state;
        if (localState != null) {
            localState.eventLoopForPartitionId(groupMetadataPartitionIndex).onResignation(
                groupMetadataPartitionIndex,
                groupMetadataPartitionLeaderEpoch
            );
        } else {
            throw new IllegalStateException("GroupCoordinator is not started.");
        }
    }

    @Override
    public Properties groupMetadataTopicConfigs() {
        Properties properties = new Properties();
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(offsetsTopicSegmentBytes));
        return properties;
    }

    @Override
    synchronized public void startup(
        IntSupplier groupMetadataTopicPartitionCountSupplier
    ) {
        if (state != null) {
            throw new IllegalStateException("Group coordinator already started.");
        }

        state = new State(
            groupMetadataTopicPartitionCountSupplier.getAsInt(),
            Collections.unmodifiableList(IntStream.range(0, numThreads).mapToObj(eventLoopId ->
                createGroupCoordinatorEventLoopBuilder.apply(eventLoopId)
                    .setTime(time)
                    .setConsumerGroupSessionTimeoutMs(consumerGroupSessionTimeoutMs)
                    .setConsumerGroupHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs)
                    .setConsumerGroupMaxSize(consumerGroupMaxSize)
                    .setConsumerGroupAssignors(consumerGroupAssignors)
                    .build()
            ).collect(Collectors.toList()))
        );
    }

    @Override
    synchronized public void shutdown() {
        State localState = state;
        state = null;

        localState.eventLoops.forEach(GroupCoordinatorEventLoop::beginShutdown);
        localState.eventLoops.forEach(eventLoop -> {
            Utils.closeQuietly(eventLoop, "event loop " + eventLoop.id());
        });
    }
}
