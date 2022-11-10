/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import kafka.server.RequestLocal
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{DeleteGroupsResponseData, DescribeGroupsResponseData, HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetDeleteRequestData, OffsetDeleteResponseData, OffsetFetchRequestData, OffsetFetchResponseData, SyncGroupRequestData, SyncGroupResponseData, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData}
import org.apache.kafka.coordinator.group.GroupCoordinatorRequestContext

import java.util
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier
import scala.jdk.CollectionConverters._

/**
 *
 */
class GroupCoordinatorAdapter(
  val coordinator: GroupCoordinator
) extends org.apache.kafka.coordinator.group.GroupCoordinator {

  override def joinGroup(
    context: GroupCoordinatorRequestContext,
    request: JoinGroupRequestData
  ): CompletableFuture[JoinGroupResponseData] = {
    val future = new CompletableFuture[JoinGroupResponseData]()
    val requestVersion = context.requestHeader.requestApiVersion
    val groupInstanceId = Option(request.groupInstanceId)

    // Only return MEMBER_ID_REQUIRED error if joinGroupRequest version is >= 4
    // and groupInstanceId is configured to unknown.
    val requireKnownMemberId = requestVersion >= 4 && groupInstanceId.isEmpty

    // let the coordinator handle join-group
    val protocols = request.protocols.valuesList.asScala.map { protocol =>
      (protocol.name, protocol.metadata)
    }.toList

    val supportSkippingAssignment = requestVersion >= 9

    def callback(joinResult: JoinGroupResult): Unit = {
      val protocolName = if (requestVersion >= 7)
        joinResult.protocolName.orNull
      else
        joinResult.protocolName.getOrElse(GroupCoordinator.NoProtocol)

      future.complete(new JoinGroupResponseData()
        .setErrorCode(joinResult.error.code)
        .setGenerationId(joinResult.generationId)
        .setProtocolType(joinResult.protocolType.orNull)
        .setProtocolName(protocolName)
        .setLeader(joinResult.leaderId)
        .setSkipAssignment(joinResult.skipAssignment)
        .setMemberId(joinResult.memberId)
        .setMembers(joinResult.members.asJava)
      )
    }

    coordinator.handleJoinGroup(
      request.groupId,
      request.memberId,
      groupInstanceId,
      requireKnownMemberId,
      supportSkippingAssignment,
      context.requestHeader.clientId,
      context.clientAddress.toString,
      request.rebalanceTimeoutMs,
      request.sessionTimeoutMs,
      request.protocolType,
      protocols,
      callback,
      Option(request.reason),
      RequestLocal(context.bufferSupplier)
    )

    future
  }

  override def syncGroup(
    context: GroupCoordinatorRequestContext,
    request: SyncGroupRequestData
  ): CompletableFuture[SyncGroupResponseData] = {
    new CompletableFuture[SyncGroupResponseData]()
  }

  override def heartbeat(
    context: GroupCoordinatorRequestContext,
    request: HeartbeatRequestData
  ): CompletableFuture[HeartbeatResponseData] = {
    new CompletableFuture[HeartbeatResponseData]()
  }

  override def fetchOffsets(
    context: GroupCoordinatorRequestContext,
    group: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
  }

  override def commitOffsets(
    context: GroupCoordinatorRequestContext,
    request: OffsetCommitRequestData
  ): CompletableFuture[OffsetCommitResponseData] = {
    new CompletableFuture[OffsetCommitResponseData]()
  }

  override def commitTransactionalOffsets(
    context: GroupCoordinatorRequestContext,
    request: TxnOffsetCommitRequestData
  ): CompletableFuture[TxnOffsetCommitResponseData] = {
    new CompletableFuture[TxnOffsetCommitResponseData]()
  }

  override def deleteOffsets(
    context: GroupCoordinatorRequestContext,
    request: OffsetDeleteRequestData
  ): CompletableFuture[OffsetDeleteResponseData] = {
    new CompletableFuture[OffsetDeleteResponseData]()
  }

  override def listGroups(
    context: GroupCoordinatorRequestContext,
    request: ListGroupsRequestData
  ): CompletableFuture[ListGroupsResponseData] = {
    new CompletableFuture[ListGroupsResponseData]()
  }

  override def describeGroup(
    context: GroupCoordinatorRequestContext,
    groupId: String
  ): CompletableFuture[DescribeGroupsResponseData.DescribedGroup] = {
    new CompletableFuture[DescribeGroupsResponseData.DescribedGroup]()
  }

  override def deleteGroup(
    context: GroupCoordinatorRequestContext,
    groupId: String
  ): CompletableFuture[DeleteGroupsResponseData.DeletableGroupResult] = {
    new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResult]()
  }

  override def partitionFor(
    groupId: String
  ): Int = {
    0
  }

  override def onElection(
    offsetTopicPartitionIndex: Int,
    offsetTopicPartitionEpoch: Int
  ): Unit = {

  }

  override def onResignation(
    offsetTopicPartitionIndex: Int,
    offsetTopicPartitionEpoch: Int
  ): Unit = {

  }

  override def onTransactionCommitted(
    producerId: Long,
    partitions: util.Collection[TopicPartition]
  ): Unit = {

  }

  override def onPartitionsDeleted(
    partitions: util.Collection[TopicPartition]
  ): Unit = {

  }

  override def startup(
    retrieveGroupMetadataTopicPartitionCount: Supplier[Integer],
    enableMetadataExpiration: Boolean
  ): Unit = {

  }

  override def shutdown(): Unit = {

  }
}
