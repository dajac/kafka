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

import kafka.server.QuotaType.ControllerMutation
import org.apache.kafka.common.metrics.Quota

import org.junit.Assert._
import org.junit.Test

class ControllerMutationQuotaManagerTest extends BaseClientQuotaManagerTest {
  private val config = ClientQuotaManagerConfig()

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
}
