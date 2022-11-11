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

import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.{JoinGroupCallback, SyncGroupCallback}
import kafka.server.RequestLocal
import org.apache.kafka.common.message.{HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData, RequestHeaderData, SyncGroupRequestData, SyncGroupResponseData}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.protocol.{ApiKeys, ByteBufferAccessor, Errors, Message, MessageUtil}
import org.apache.kafka.common.requests.{SyncGroupRequest, SyncGroupResponse}
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.coordinator.group.GroupCoordinatorRequestContext
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, reset, verify}

import java.net.InetAddress
import scala.collection.Map
import scala.jdk.CollectionConverters._

class GroupCoordinatorAdapterTest {

  private def makeContext(
    apiKeys: ApiKeys,
    apiVersion: Short = -1
  ): GroupCoordinatorRequestContext = {
    new GroupCoordinatorRequestContext(
      new RequestHeaderData()
        .setRequestApiKey(apiKeys.id)
        .setRequestApiVersion(if (apiVersion > -1) apiVersion else apiKeys.latestVersion)
        .setClientId("client")
        .setCorrelationId(0),
      InetAddress.getLocalHost,
      BufferSupplier.create()
    )
  }

  @Test
  def testJoinGroupProtocolsOrder(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val protocols = List(
      ("first", "first".getBytes()),
      ("second", "second".getBytes())
    )

    val capturedProtocols: ArgumentCaptor[List[(String, Array[Byte])]] =
      ArgumentCaptor.forClass(classOf[List[(String, Array[Byte])]])

    val ctx = makeContext(apiKeys = ApiKeys.JOIN_GROUP)
    val data = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
        protocols.map { case (name, protocol) => new JoinGroupRequestProtocol()
          .setName(name)
          .setMetadata(protocol)
        }.iterator.asJava))
    adapter.joinGroup(ctx, data)

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(ctx.requestHeader.clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(data.rebalanceTimeoutMs),
      ArgumentMatchers.eq(data.sessionTimeoutMs),
      ArgumentMatchers.eq(data.protocolType),
      capturedProtocols.capture(),
      any(),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(RequestLocal(ctx.bufferSupplier))
    )

    val capturedProtocolsList = capturedProtocols.getValue
    assertEquals(protocols, capturedProtocolsList)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroupWhenAnErrorOccurs(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(apiKeys = ApiKeys.JOIN_GROUP, apiVersion = version)
    val data = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
    val future = adapter.joinGroup(ctx, data)

    val capturedCallback: ArgumentCaptor[JoinGroupCallback] =
      ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(ctx.requestHeader.clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(if (version >= 1) data.rebalanceTimeoutMs else data.sessionTimeoutMs),
      ArgumentMatchers.eq(data.sessionTimeoutMs),
      ArgumentMatchers.eq(data.protocolType),
      ArgumentMatchers.eq(List.empty),
      capturedCallback.capture(),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(RequestLocal(ctx.bufferSupplier))
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(JoinGroupResult(data.memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))

    val expectedData = new JoinGroupResponseData()
      .setMemberId(data.memberId)
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setProtocolName(if (version >= 7) null else GroupCoordinator.NoProtocol)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroupProtocolType(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(apiKeys = ApiKeys.JOIN_GROUP, apiVersion = version)
    val data = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val future = adapter.joinGroup(ctx, data)

    val capturedCallback: ArgumentCaptor[JoinGroupCallback] =
      ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(ctx.requestHeader.clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(if (version >= 1) data.rebalanceTimeoutMs else data.sessionTimeoutMs),
      ArgumentMatchers.eq(data.sessionTimeoutMs),
      ArgumentMatchers.eq(data.protocolType),
      ArgumentMatchers.eq(List.empty),
      capturedCallback.capture(),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(RequestLocal(ctx.bufferSupplier))
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(JoinGroupResult(
      members = List.empty,
      memberId = data.memberId,
      generationId = 0,
      protocolType = Some(data.protocolType),
      protocolName = Some("range"),
      leaderId = data.memberId,
      skipAssignment = true,
      error = Errors.NONE
    ))

    val expectedData = new JoinGroupResponseData()
      .setMemberId(data.memberId)
      .setGenerationId(0)
      .setProtocolType(data.protocolType)
      .setProtocolName("range")
      .setLeader(data.memberId)
      .setSkipAssignment(true)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testSyncGroupProtocolTypeAndName(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(apiKeys = ApiKeys.SYNC_GROUP, apiVersion = version)
    val data = new SyncGroupRequestData()
      .setGroupId("group")
      .setGenerationId(0)
      .setMemberId("member1")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val future = adapter.syncGroup(ctx, data)

    val capturedCallback: ArgumentCaptor[SyncGroupCallback] =
      ArgumentCaptor.forClass(classOf[SyncGroupCallback])

    verify(groupCoordinator).handleSyncGroup(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.generationId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(if (version >= 5) Some(data.protocolType) else None),
      ArgumentMatchers.eq(if (version >= 5) Some(data.protocolName) else None),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(Map.empty),
      capturedCallback.capture(),
      ArgumentMatchers.eq(RequestLocal(ctx.bufferSupplier))
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(SyncGroupResult(
      protocolType = Some(data.protocolType),
      protocolName = Some(data.protocolName),
      memberAssignment = Array.empty,
      error = Errors.NONE
    ))

    val expectedData = new SyncGroupResponseData()
      .setProtocolName(data.protocolName)
      .setProtocolType(data.protocolType)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @Test
  def testHeartbeat(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(apiKeys = ApiKeys.SYNC_GROUP)
    val data = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member1")
      .setGenerationId(0)

    val future = adapter.heartbeat(ctx, data)

    val capturedCallback: ArgumentCaptor[Errors => Unit] =
      ArgumentCaptor.forClass(classOf[Errors => Unit])

    verify(groupCoordinator).handleHeartbeat(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(data.generationId),
      capturedCallback.capture(),
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(Errors.NONE)

    val expectedData = new HeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

}
