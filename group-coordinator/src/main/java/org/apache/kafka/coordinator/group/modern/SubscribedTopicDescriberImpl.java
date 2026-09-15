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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The subscribed topic metadata class is used by the {@link PartitionAssignor} to obtain
 * topic and partition metadata for the topics that the modern group is subscribed to.
 */
public class SubscribedTopicDescriberImpl implements SubscribedTopicDescriber {
    /**
     * The metadata image that contains the latest metadata information.
     */
    private final CoordinatorMetadataImage metadataImage;

    /**
     * The topic whose racks were looked up last. Assignors ask for the racks of the partitions
     * of a topic one after the other, so remembering the last topic saves one lookup in the
     * image per partition. Only {@link #racksForPartition} uses it: {@link #numPartitions} is
     * asked once per topic, so remembering it there would only cost an entry per topic. The
     * entry is immutable and replaced as a whole, so a stale read still gives a consistent answer.
     */
    private LastTopic lastTopic;

    private record LastTopic(Uuid topicId, Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadata) { }

    public SubscribedTopicDescriberImpl(CoordinatorMetadataImage metadataImage) {
        this.metadataImage = Objects.requireNonNull(metadataImage);
    }

    private Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadata(Uuid topicId) {
        LastTopic last = lastTopic;
        if (last != null && last.topicId.equals(topicId)) {
            return last.topicMetadata;
        }
        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadata = metadataImage.topicMetadata(topicId);
        lastTopic = new LastTopic(topicId, topicMetadata);
        return topicMetadata;
    }

    /**
     * The number of partitions for the given topic Id.
     *
     * @param topicId   Uuid corresponding to the topic.
     * @return The number of partitions corresponding to the given topic Id,
     *         or -1 if the topic Id does not exist.
     */
    @Override
    public int numPartitions(Uuid topicId) {
        return metadataImage.topicMetadata(topicId).map(CoordinatorMetadataImage.TopicMetadata::partitionCount).orElse(-1);
    }

    /**
     * Returns all the available racks associated with the replicas of the given partition.
     *
     * @param topicId       Uuid corresponding to the partition's topic.
     * @param partition     Partition Id within the topic.
     * @return The set of racks corresponding to the replicas of the topics partition.
     *         If the topic Id does not exist or no partition rack information is available, an empty set is returned.
     */
    @Override
    public Set<String> racksForPartition(Uuid topicId, int partition) {
        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadataOp = topicMetadata(topicId);
        if (topicMetadataOp.isEmpty()) {
            return Set.of();
        }

        CoordinatorMetadataImage.TopicMetadata topicMetadata = topicMetadataOp.get();
        List<String> racks = topicMetadata.partitionRacks(partition);
        if (racks == null || racks.isEmpty()) {
            return Set.of();
        }
        // The replicas of a partition are in a handful of racks, so the distinct racks are found
        // by comparison rather than through a hash table, and returned in a compact immutable set.
        String[] distinct = new String[racks.size()];
        int count = 0;
        for (String rack : racks) {
            boolean seen = false;
            for (int i = 0; i < count && !seen; i++) {
                seen = distinct[i].equals(rack);
            }
            if (!seen) {
                distinct[count++] = rack;
            }
        }
        return switch (count) {
            case 1 -> Set.of(distinct[0]);
            case 2 -> Set.of(distinct[0], distinct[1]);
            default -> Set.of(count == distinct.length ? distinct : Arrays.copyOf(distinct, count));
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribedTopicDescriberImpl that = (SubscribedTopicDescriberImpl) o;
        return metadataImage.equals(that.metadataImage);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(metadataImage);
    }

    @Override
    public String toString() {
        return "SubscribedTopicMetadata(" +
            "metadataImage=" + metadataImage +
            ')';
    }
}
