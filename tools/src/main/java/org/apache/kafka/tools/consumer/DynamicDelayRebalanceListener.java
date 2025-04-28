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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.nio.file.*;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class DynamicDelayRebalanceListener implements ConsumerRebalanceListener {
    private final String clientId;

    public DynamicDelayRebalanceListener(String clientId) {
        this.clientId = clientId;
    }

    private int getDelaySeconds() {
        Path path = Paths.get("/tmp", clientId, "rebalance_delay");
        if (Files.exists(path)) {
            try {
                List<String> lines = Files.readAllLines(path);
                if (!lines.isEmpty()) {
                    return Integer.parseInt(lines.get(0).trim());
                }
            } catch (IOException | NumberFormatException e) {
                // Ignore
            }
        }
        return 0;
    }

    private void maybeDelay() {
        int delaySeconds = getDelaySeconds();
        if (delaySeconds > 0) {
            try {
                Thread.sleep(delaySeconds * 1000L);
            } catch (InterruptedException ignored) {
                // Ignore
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        //maybeDelay();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        maybeDelay();
    }
}

