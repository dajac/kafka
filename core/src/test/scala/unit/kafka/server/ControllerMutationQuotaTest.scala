/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package kafka.server

import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

import kafka.utils.TestUtils
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.requests.AlterClientQuotasRequest
import org.apache.kafka.common.requests.AlterClientQuotasResponse
import org.apache.kafka.common.requests.CreatePartitionsRequest
import org.apache.kafka.common.requests.CreatePartitionsResponse
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.requests.CreateTopicsResponse
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.requests.DeleteTopicsResponse
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

import scala.jdk.CollectionConverters._

object ControllerMutationQuotaTest {
  // Principal used for all client connections. This is updated by each test.
  var principal = KafkaPrincipal.ANONYMOUS
  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      principal
    }
  }

  def asPrincipal(newPrincipal: KafkaPrincipal)(f: => Unit): Unit = {
    val currentPrincipal = principal
    principal = newPrincipal
    try f
    finally principal = currentPrincipal
  }

  val ThrottledPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ThrottledPrincipal")
  val UnboundedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "UnboundedPrincipal")

  val StrictCreateTopicsRequestVersion = ApiKeys.CREATE_TOPICS.latestVersion
  val PermissiveCreateTopicsRequestVersion = 5.toShort

  val StrictDeleteTopicsRequestVersion = ApiKeys.DELETE_TOPICS.latestVersion
  val PermissiveDeleteTopicsRequestVersion = 4.toShort

  val StrictCreatePartitionsRequestVersion = ApiKeys.CREATE_PARTITIONS.latestVersion
  val PermissiveCreatePartitionsRequestVersion = 2.toShort
}

class ControllerMutationQuotaTest extends BaseRequestTest {
  import ControllerMutationQuotaTest._

  override def brokerCount: Int = 1

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.PrincipalBuilderClassProp,
      classOf[ControllerMutationQuotaTest.TestPrincipalBuilder].getName)
    // We use the default number of samples and window size.
    properties.put(KafkaConfig.NumControllerQuotaSamplesProp, "11")
    properties.put(KafkaConfig.ControllerQuotaWindowSizeSecondsProp, "1")
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()

    // Define a quota for ThrottledPrincipal
    defineUserQuota(ThrottledPrincipal.getName, Some(2.0))
    waitUserQuota(ThrottledPrincipal.getName, 2.0)
  }

  @Test
  def testStrictCreateTopicsRequest(): Unit = {
    asPrincipal(ThrottledPrincipal) {
      val topics = Seq("topic-1" -> 30, "topic-2" -> 30)

      // Create two topics worth of 30 partitions each. As we use a strict quota, we
      // expect the first topic to be created and the second to be rejected.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((30 / 10) - 2) / 2 * 10 = 5s
      val (throttleTimeMs1, errors1) = createTopics(topics, StrictCreateTopicsRequestVersion)
      assertTrue((5000 - throttleTimeMs1) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1)

      // The implementation of the Rate has NOT been changed yet so we have to wait past
      // the window in order to get the avg rate bellow the quota.
      Thread.sleep(11000) // Thread.sleep(throttleTimeMs1)

      // Retry the second topic after the throttling delay is passed. It should
      // succeed and the throttle time should be zero.
      val (throttleTimeMs2, errors2) = createTopics(topics.drop(1), StrictCreateTopicsRequestVersion)
      assertEquals(0, throttleTimeMs2)
      assertEquals(Seq(Errors.NONE), errors2)
    }
  }

  @Test
  def testPermissiveCreateTopicsRequest(): Unit = {
    asPrincipal(ThrottledPrincipal) {
      val topics = Seq("topic-1" -> 30, "topic-2" -> 30)

      // Create two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be created.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((60 / 10) - 2) / 2 * 10 = 20s
      val (throttleTimeMs, errors) = createTopics(topics, PermissiveCreateTopicsRequestVersion)
      assertTrue((20000 - throttleTimeMs) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedCreateTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      val topics = Seq("topic-1" -> 30, "topic-2" -> 30)

      // Create two topics worth of 30 partitions each. As we use an user without quota, we
      // expect both topics to be created. The throttle time should be equal to 0.
      val (throttleTimeMs, errors) = createTopics(topics, StrictCreateTopicsRequestVersion)
      assertEquals(0, throttleTimeMs)
      assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
    }
  }

  @Test
  def testStrictDeleteTopicsRequest(): Unit = {
    val topics = Seq("topic-1" -> 30, "topic-2" -> 30)

    // Create topics with unbounded principal
    asPrincipal(UnboundedPrincipal) {
      createTopics(topics, StrictCreateTopicsRequestVersion)
    }

    // Switch to throttled principal
    asPrincipal(ThrottledPrincipal) {
      // Delete two topics worth of 30 partitions each. As we use a strict quota, we
      // expect the first topic to be deleted and the second to be rejected.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((30 / 10) - 2) / 2 * 10 = 5s
      val (throttleTimeMs1, errors1) = deleteTopics(topics, StrictDeleteTopicsRequestVersion)
      assertTrue((5000 - throttleTimeMs1) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1)

      // The implementation of the Rate has NOT been changed yet so we have to wait past
      // the window in order to get the avg rate bellow the quota.
      Thread.sleep(11000) // Thread.sleep(throttleTimeMs1)

      // Retry the second topic after the throttling delay is passed. It should
      // succeed and the throttle time should be zero.
      val (throttleTimeMs2, errors2) = deleteTopics(topics.drop(1), StrictDeleteTopicsRequestVersion)
      assertEquals(0, throttleTimeMs2)
      assertEquals(Seq(Errors.NONE), errors2)
    }
  }

  @Test
  def testPermissiveDeleteTopicsRequest(): Unit = {
    val topics = Seq("topic-1" -> 30, "topic-2" -> 30)

    // Create topics with unbounded principal
    asPrincipal(UnboundedPrincipal) {
      createTopics(topics, StrictCreateTopicsRequestVersion)
    }

    // Switch to throttled principal
    asPrincipal(ThrottledPrincipal) {
      // Delete two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be deleted.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((60 / 10) - 2) / 2 * 10 = 20s
      val (throttleTimeMs, errors) = deleteTopics(topics, PermissiveDeleteTopicsRequestVersion)
      assertTrue((20000 - throttleTimeMs) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedDeleteTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      val topics = Seq("topic-1" -> 30, "topic-2" -> 30)
      createTopics(topics, StrictCreateTopicsRequestVersion)

      // Delete two topics worth of 30 partitions each. As we use an user without quota, we
      // expect both topics to be deleted. The throttle time should be equal to 0.
      val (throttleTimeMs, errors) = deleteTopics(topics, StrictDeleteTopicsRequestVersion)
      assertEquals(0, throttleTimeMs)
      assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
    }
  }

  @Test
  def testStrictCreatePartitionsRequest(): Unit = {
    // Create topics with unbounded principal
    asPrincipal(UnboundedPrincipal) {
      createTopics(Seq("topic-1" -> 1, "topic-2" -> 1), StrictCreatePartitionsRequestVersion)
    }

    // Switch to throttled principal
    asPrincipal(ThrottledPrincipal) {
      val topics = Seq("topic-1" -> 31, "topic-2" -> 31)

      // Add 30 partitions to each topic. As we use a strict quota, we
      // expect the first topic to be extended and the second to be rejected.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((30 / 10) - 2) / 2 * 10 = 5s
      val (throttleTimeMs1, errors1) = createPartitions(topics, StrictCreatePartitionsRequestVersion)
      println(throttleTimeMs1)
      println(errors1)
      assertTrue((5000 - throttleTimeMs1) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1)

      // The implementation of the Rate has NOT been changed yet so we have to wait past
      // the window in order to get the avg rate bellow the quota.
      Thread.sleep(11000) // Thread.sleep(throttleTimeMs1)

      // Retry the second topic after the throttling delay is passed. It should
      // succeed and the throttle time should be zero.
      val (throttleTimeMs2, errors2) = deleteTopics(topics.drop(1), StrictCreatePartitionsRequestVersion)
      assertEquals(0, throttleTimeMs2)
      assertEquals(Seq(Errors.NONE), errors2)
    }
  }

  @Test
  def testPermissiveCreatePartitionsRequest(): Unit = {
    // Create topics with unbounded principal
    asPrincipal(UnboundedPrincipal) {
      createTopics(Seq("topic-1" -> 1, "topic-2" -> 1), StrictCreatePartitionsRequestVersion)
    }

    // Switch to throttled principal
    asPrincipal(ThrottledPrincipal) {
      val topics = Seq("topic-1" -> 31, "topic-2" -> 31)

      // Create two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be created.
      // Theoretically, the throttle time should be bellow or equal to:
      // ((60 / 10) - 2) / 2 * 10 = 20s
      val (throttleTimeMs, errors) = createPartitions(topics, PermissiveCreatePartitionsRequestVersion)
      assertTrue((20000 - throttleTimeMs) < 1000)
      assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedCreatePartitionsRequest(): Unit = {
    ControllerMutationQuotaTest.principal = UnboundedPrincipal
    createTopics(Seq(("topic-1", 1), ("topic-2", 1)), StrictCreatePartitionsRequestVersion)

    val (topic1, topic2) = (("topic-1", 31), ("topic-2", 31))

    // Create two topics worth of 30 partitions each. As we use an user without quota, we
    // expect both topics to be created. The throttle time should be equal to 0.
    val (throttleTimeMs, errors) = deleteTopics(Seq(topic1, topic2), StrictCreatePartitionsRequestVersion)
    assertEquals(0, throttleTimeMs)
    assertEquals(Seq(Errors.NONE, Errors.NONE), errors)
  }

  private def createTopics(topics: Seq[(String, Int)], version: Short): (Int, Seq[Errors]) = {
    val data = new CreateTopicsRequestData()
    topics.foreach { case (topic, numPartitions) =>
      data.topics.add(new CreatableTopic()
        .setName(topic).setNumPartitions(numPartitions).setReplicationFactor(1))
    }
    val request = new CreateTopicsRequest.Builder(data).build(version)
    val response = connectAndReceive[CreateTopicsResponse](request)
    // We assume that order of topics is preserved
    response.data.throttleTimeMs -> response.data.topics.asScala
      .map(topic => Errors.forCode(topic.errorCode)).toSeq
  }

  private def deleteTopics(topics: Seq[(String, Int)], version: Short): (Int, Seq[Errors]) = {
    val data = new DeleteTopicsRequestData()
      .setTimeoutMs(60000)
      .setTopicNames(topics.map(_._1).asJava)
    val request = new DeleteTopicsRequest.Builder(data).build(version)
    val response = connectAndReceive[DeleteTopicsResponse](request)
    // We assume that order of topics is preserved
    response.data.throttleTimeMs -> response.data.responses.asScala
      .map(topic => Errors.forCode(topic.errorCode)).toSeq
  }

  private def createPartitions(topics: Seq[(String, Int)], version: Short): (Int, Seq[Errors]) = {
    val data = new CreatePartitionsRequestData().setTimeoutMs(60000)
    topics.foreach { case (topic, numPartitions) =>
      data.topics.add(new CreatePartitionsTopic().setName(topic).setCount(numPartitions))
    }
    val request = new CreatePartitionsRequest.Builder(data).build(version)
    val response = connectAndReceive[CreatePartitionsResponse](request)
    println(response.data())
    // We assume that order of topics is preserved
    response.data.throttleTimeMs -> response.data.results.asScala
      .map(topic => Errors.forCode(topic.errorCode)).toSeq
  }

  private def defineUserQuota(user: String, quota: Option[Double]): Unit = {
    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> user).asJava)
    val quotas = Map(DynamicConfig.Client.ControllerMutationOverrideProp -> quota)

    try alterClientQuotas(Map(entity -> quotas))(entity).get(10, TimeUnit.SECONDS) catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def waitUserQuota(user: String, expectedQuota: Double): Unit = {
    val quotaManager = servers.head.quotaManagers.controllerMutation
    var actualQuota = Double.MinValue

    TestUtils.waitUntilTrue(() => {
      actualQuota = quotaManager.quota(user, "").bound()
      expectedQuota == actualQuota
    }, s"Quota of $user is not $expectedQuota but $actualQuota")
  }

  private def alterClientQuotas(request: Map[ClientQuotaEntity, Map[String, Option[Double]]]): Map[ClientQuotaEntity, KafkaFutureImpl[Void]] = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new ClientQuotaAlteration.Op(key, value.map(Double.box).orNull)
      }.asJavaCollection
      new ClientQuotaAlteration(entity, ops)
    }

    val response = request.map(e => e._1 -> new KafkaFutureImpl[Void]).asJava
    sendAlterClientQuotasRequest(entries).complete(response)
    val result = response.asScala
    assertEquals(request.size, result.size)
    request.foreach(e => assertTrue(result.get(e._1).isDefined))
    result.toMap
  }

  private def sendAlterClientQuotasRequest(entries: Iterable[ClientQuotaAlteration]): AlterClientQuotasResponse = {
    val request = new AlterClientQuotasRequest.Builder(entries.asJavaCollection, false).build()
    connectAndReceive[AlterClientQuotasResponse](request, destination = controllerSocketServer)
  }
}
