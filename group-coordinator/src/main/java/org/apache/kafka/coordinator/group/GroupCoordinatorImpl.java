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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.FutureUtils;

import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

public class GroupCoordinatorImpl implements GroupCoordinator {
    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
//        TopicPartition tp = partitionFor(request.groupId);
//        return runtime.scheduleWrite("heartbeat", tp, (stateMachine) ->
//            stateMachine.consumerGroupHeartbeat(context, request));
        return FutureUtils.failedFuture(Errors.UNSUPPORTED_VERSION.exception(
            "This API is not implemented yet."
        ));
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
    public int partitionFor(
        String groupId
    ) {
        return 0;
    }

    @Override
    public void onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    ) {

    }

    @Override
    public void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    ) {

    }

    @Override
    public void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    ) {

    }

    @Override
    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {

    }

    @Override
    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {

    }

    @Override
    public Properties groupMetadataTopicConfigs() {
        return null;
    }

    @Override
    public void startup(
        IntSupplier groupMetadataTopicPartitionCount
    ) {

    }

    @Override
    public void shutdown() {

    }
}
