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

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor.QuotaEnforcementType
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.quota.ClientQuotaCallback

import scala.jdk.CollectionConverters._

class ControllerMutationQuotaManager(private val config: ClientQuotaManagerConfig,
                                     private val metrics: Metrics,
                                     private val time: Time,
                                     private val threadNamePrefix: String,
                                     private val quotaCallback: Option[ClientQuotaCallback])
    extends ClientQuotaManager(config, metrics, QuotaType.ControllerMutation, QuotaEnforcementType.STRICT,
      time, threadNamePrefix, quotaCallback) {

  override protected def clientRateMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("mutation-rate", QuotaType.ControllerMutation.toString,
      "Tracking mutation-rate per user/client-id",
      quotaMetricTags.asJava)
  }
}
