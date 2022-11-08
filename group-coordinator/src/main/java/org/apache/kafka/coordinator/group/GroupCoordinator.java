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
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface GroupCoordinator {

    /**
     *
     */
    CompletableFuture<JoinGroupResponseData> joinGroup(
        GroupCoordinatorRequestContext context,
        JoinGroupRequestData request
    );

    /**
     *
     */
    CompletableFuture<SyncGroupResponseData> syncGroup(
        GroupCoordinatorRequestContext context,
        SyncGroupRequestData request
    );

    /**
     *
     */
    CompletableFuture<HeartbeatResponseData> heartbeat(
        GroupCoordinatorRequestContext context,
        HeartbeatRequestData request
    );

    /**
     *
     */
    CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(
        GroupCoordinatorRequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup group,
        boolean requireStable
    );

    /**
     *
     */
    CompletableFuture<OffsetCommitResponseData> commitOffsets(
        GroupCoordinatorRequestContext context,
        OffsetCommitRequestData request
    );

    /**
     *
     */
    CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        GroupCoordinatorRequestContext context,
        TxnOffsetCommitRequestData request
    );

    /**
     *
     */
    CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        GroupCoordinatorRequestContext context,
        OffsetDeleteRequestData request
    );

    /**
     *
     */
    CompletableFuture<ListGroupsResponseData> listGroups(
        GroupCoordinatorRequestContext context,
        ListGroupsRequestData request
    );

    /**
     *
     */
    CompletableFuture<DescribeGroupsResponseData.DescribedGroup> describeGroup(
        GroupCoordinatorRequestContext context,
        String groupId
    );

    /**
     *
     */
    CompletableFuture<DeleteGroupsResponseData.DeletableGroupResult> deleteGroup(
        GroupCoordinatorRequestContext context,
        String groupId
    );

    /**
     *
     */
    int partitionFor(
        String groupId
    );

    /**
     *
     */
    void onElection(
        int offsetTopicPartitionIndex,
        int offsetTopicPartitionEpoch
    );

    /**
     *
     */
    void onResignation(
        int offsetTopicPartitionIndex,
        int offsetTopicPartitionEpoch
    );

    /**
     *
     */
    void onTransactionCommitted(
        long producerId,
        Collection<TopicPartition> partitions
    );

    /**
     *
     */
    void onPartitionsDeleted(
        Collection<TopicPartition> partitions
    );

    /**
     *
     */
    void startup(
        Supplier<Integer> retrieveGroupMetadataTopicPartitionCount,
        boolean enableMetadataExpiration
    );

    /**
     *
     */
    void shutdown();
}

