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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a metadata image with explicit broker racks and partition replicas, for rack aware
 * assignor tests.
 */
public final class TestMetadataImageBuilder {
    private final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();

    public TestMetadataImageBuilder addBroker(int brokerId, String rack) {
        delta.replay(new RegisterBrokerRecord().setBrokerId(brokerId).setRack(rack));
        return this;
    }

    /**
     * Adds a topic with the given replicas per partition.
     */
    public TestMetadataImageBuilder addTopic(Uuid topicId, String name, List<List<Integer>> replicasPerPartition) {
        delta.replay(new TopicRecord().setTopicId(topicId).setName(name));
        for (int i = 0; i < replicasPerPartition.size(); i++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(i)
                .setReplicas(replicasPerPartition.get(i)));
        }
        return this;
    }

    /**
     * Adds a topic whose partition {@code i} has replicas on brokers {@code i % numBrokers},
     * {@code (i + 1) % numBrokers}, ... up to the replication factor.
     */
    public TestMetadataImageBuilder addTopic(Uuid topicId, String name, int numPartitions, int numBrokers, int replicationFactor) {
        delta.replay(new TopicRecord().setTopicId(topicId).setName(name));
        for (int i = 0; i < numPartitions; i++) {
            List<Integer> replicas = new ArrayList<>();
            for (int r = 0; r < Math.min(replicationFactor, numBrokers); r++) {
                replicas.add((i + r) % numBrokers);
            }
            delta.replay(new PartitionRecord().setTopicId(topicId).setPartitionId(i).setReplicas(replicas));
        }
        return this;
    }

    /**
     * @return The image of the brokers and topics added so far.
     */
    public CoordinatorMetadataImage buildImage() {
        MetadataImage image = delta.apply(new MetadataProvenance(0, 0, 0L, true));
        return new KRaftCoordinatorMetadataImage(image);
    }

    /**
     * @return A describer of the image of the brokers and topics added so far.
     */
    public SubscribedTopicDescriberImpl buildDescriber() {
        return new SubscribedTopicDescriberImpl(buildImage());
    }
}
