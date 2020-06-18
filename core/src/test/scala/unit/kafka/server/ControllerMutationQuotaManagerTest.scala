/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.net.InetAddress
import java.util
import java.util.Collections

import kafka.network.RequestChannel
import kafka.network.RequestChannel.EndThrottlingResponse
import kafka.network.RequestChannel.Session
import kafka.network.RequestChannel.StartThrottlingResponse
import kafka.server.QuotaType.ControllerMutation
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.easymock.EasyMock
import org.junit.After
import org.junit.Assert._
import org.junit.Test

class ControllerMutationQuotaManagerTest {
  private val time = new MockTime
  private val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
  private val config = ClientQuotaManagerConfig()

  var numCallbacks: Int = 0

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  def callback(response: RequestChannel.Response): Unit = {
    // Count how many times this callback is called for notifyThrottlingDone().
    response match {
      case _: StartThrottlingResponse =>
      case _: EndThrottlingResponse => numCallbacks += 1
    }
  }

  @Test
  def testControllerMutationQuotaViolation(): Unit = {
    val controllerMutationQuotaManager = new ControllerMutationQuotaManager(config, metrics, time,"", None)
    controllerMutationQuotaManager.updateQuota(Some("ANONYMOUS"), Some("test-client"), Some("test-client"),
      Some(Quota.upperBound(10)))
    val queueSizeMetric = metrics.metrics().get(
      metrics.metricName("queue-size", ControllerMutation.toString, ""))

    try {
      // Verify that there is no quota violation if we remain under the quota.
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(controllerMutationQuotaManager, "ANONYMOUS", "test-client", 10))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // Create a spike worth of 110 mutations.
      // Current avg rate = 10 * 10 = 100/10 = 10 mutations per second.
      // As we use the Strict enforcement, the quota is checked before updating the rate. Hence,
      // the spike is accepted and no quota violation error is raised.
      var throttleTime = maybeRecord(controllerMutationQuotaManager, "ANONYMOUS", "test-client", 110)
      assertEquals("Should not be throttled", 0, throttleTime)

      // Create a spike worth of 110 mutations.
      // Current avg rate = 10 * 10 + 110 = 210/10 = 21 mutations per second.
      // As the quota is already violated, the spike is rejected immediately without updating the
      // rate. The client must wait:
      // (21 - quota) / quota * window-size = (21 - 10) / 10 * 10 = 11 seconds
      throttleTime = maybeRecord(controllerMutationQuotaManager, "ANONYMOUS", "test-client", 110)
      assertEquals("Should be throttled", 11000, throttleTime)

      // Throttle
      throttle(controllerMutationQuotaManager, "ANONYMOUS", "unknown", throttleTime, callback)
      assertEquals(1, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // After a request is delayed, the callback cannot be triggered immediately
      controllerMutationQuotaManager.throttledChannelReaper.doWork()
      assertEquals(0, numCallbacks)

      // Callback can only be triggered after the delay time passes
      time.sleep(throttleTime)
      controllerMutationQuotaManager.throttledChannelReaper.doWork()
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      assertEquals(1, numCallbacks)

      // Retry to spike worth of 110 mutations after having waited the required throttle time.
      // Current avg rate = 0 = 0/11 = 0 mutations per second.
      throttleTime = maybeRecord(controllerMutationQuotaManager, "ANONYMOUS", "test-client", 110)
      assertEquals("Should be throttled", 0, throttleTime)
    } finally {
      controllerMutationQuotaManager.shutdown()
    }
  }

  private def maybeRecord(quotaManager: ClientQuotaManager, user: String, clientId: String, value: Double): Int = {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    quotaManager.maybeRecordAndGetThrottleTimeMs(Session(principal, null), clientId, value, time.milliseconds())
  }

  private def throttle(quotaManager: ClientQuotaManager, user: String, clientId: String, throttleTimeMs: Int,
                       channelThrottlingCallback: RequestChannel.Response => Unit): Unit = {
    val (_, request) = buildRequest(FetchRequest.Builder.forConsumer(0, 1000, new util.HashMap[TopicPartition, PartitionData]))
    quotaManager.throttle(request, throttleTimeMs, channelThrottlingCallback)
  }

  private def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T],
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): (T, RequestChannel.Request) = {

    val request = builder.build()
    val buffer = request.serialize(new RequestHeader(builder.apiKey, request.version, "", 0))
    val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos =  0, MemoryPool.NONE, buffer,
      requestChannelMetrics))
  }
}
