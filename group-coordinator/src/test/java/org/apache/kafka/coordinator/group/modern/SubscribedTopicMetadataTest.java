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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private SubscribedTopicDescriberImpl subscribedTopicMetadata;
    private CoordinatorMetadataImage metadataImage;
    private final int numPartitions = 5;

    @BeforeEach
    public void setUp() {
        MetadataImageBuilder metadataImageBuilder = new MetadataImageBuilder();
        for (int i = 0; i < 5; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            metadataImageBuilder.addTopic(topicId, topicName, numPartitions);
        }
        metadataImage = metadataImageBuilder.addRacks().buildCoordinatorMetadataImage();

        subscribedTopicMetadata = new SubscribedTopicDescriberImpl(metadataImage);
    }

    @Test
    public void testMetadataImageCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic ID doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        // Test that the correct number of partitions are returned for a given topic ID.
        metadataImage.topicIds().forEach(id ->
            // Test that the correct number of partitions are returned for a given topic ID.
            assertEquals(numPartitions, subscribedTopicMetadata.numPartitions(id))
        );
    }

    @Test
    public void testRacksForPartition() {
        Uuid topicId = Uuid.randomUuid();

        // Test empty set is returned when the topic ID doesn't exist.
        assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(topicId, 0));
        metadataImage.topicIds().forEach(id -> {
            // Test empty set is returned when the partition ID doesn't exist.
            assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(id, 10));

            // Test that the correct racks of partition are returned for a given topic ID.
            assertEquals(Set.of("rack0", "rack1"), subscribedTopicMetadata.racksForPartition(id, 0));
        });
    }

    @Test
    public void testRacksForPartitionAlternatingTopics() {
        // The describer remembers the last topic looked up. Alternating topics and partitions
        // must still give the racks of the right partition of the right topic.
        List<Uuid> topicIds = new ArrayList<>(metadataImage.topicIds());
        for (int partition = 0; partition < numPartitions; partition++) {
            for (Uuid id : topicIds) {
                assertEquals(
                    Set.of("rack" + (partition % 4), "rack" + ((partition + 1) % 4)),
                    subscribedTopicMetadata.racksForPartition(id, partition)
                );
                assertEquals(numPartitions, subscribedTopicMetadata.numPartitions(id));
            }
            assertEquals(Set.of(), subscribedTopicMetadata.racksForPartition(Uuid.randomUuid(), partition));
        }
    }

    @Test
    public void testRacksForPartitionDistinctRacks() {
        Uuid topicId = Uuid.randomUuid();
        Uuid otherTopicId = Uuid.randomUuid();
        AtomicInteger lookups = new AtomicInteger();
        List<List<String>> racksByPartition = List.of(
            List.of(),
            List.of("rack0"),
            List.of("rack0", "rack0"),
            List.of("rack1", "rack0"),
            List.of("rack0", "rack1", "rack0", "rack2"),
            List.of("rack0", "rack1", "rack2", "rack3", "rack4")
        );
        CoordinatorMetadataImage image = new CoordinatorMetadataImage() {
            @Override
            public Set<Uuid> topicIds() {
                return Set.of(topicId, otherTopicId);
            }

            @Override
            public Set<String> topicNames() {
                return Set.of("topic", "other");
            }

            @Override
            public Optional<TopicMetadata> topicMetadata(String topicName) {
                return Optional.empty();
            }

            @Override
            public Optional<TopicMetadata> topicMetadata(Uuid id) {
                lookups.incrementAndGet();
                if (!id.equals(topicId) && !id.equals(otherTopicId)) {
                    return Optional.empty();
                }
                return Optional.of(new TopicMetadata() {
                    @Override
                    public String name() {
                        return id.equals(topicId) ? "topic" : "other";
                    }

                    @Override
                    public Uuid id() {
                        return id;
                    }

                    @Override
                    public int partitionCount() {
                        return racksByPartition.size() + 1;
                    }

                    @Override
                    public List<String> partitionRacks(int partition) {
                        if (id.equals(otherTopicId)) {
                            return List.of("other-rack");
                        }
                        // The last partition has no rack information at all.
                        return partition < racksByPartition.size() ? racksByPartition.get(partition) : null;
                    }
                });
            }

            @Override
            public CoordinatorMetadataDelta emptyDelta() {
                return CoordinatorMetadataDelta.EMPTY;
            }

            @Override
            public long version() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        };

        SubscribedTopicDescriberImpl describer = new SubscribedTopicDescriberImpl(image);
        assertEquals(Set.of(), describer.racksForPartition(topicId, 0));
        assertEquals(Set.of("rack0"), describer.racksForPartition(topicId, 1));
        assertEquals(Set.of("rack0"), describer.racksForPartition(topicId, 2));
        assertEquals(Set.of("rack0", "rack1"), describer.racksForPartition(topicId, 3));
        assertEquals(Set.of("rack0", "rack1", "rack2"), describer.racksForPartition(topicId, 4));
        assertEquals(Set.of("rack0", "rack1", "rack2", "rack3", "rack4"), describer.racksForPartition(topicId, 5));
        assertEquals(Set.of(), describer.racksForPartition(topicId, 6));
        // The partitions of a topic asked one after the other cost a single lookup in the image.
        assertEquals(1, lookups.get());
        // numPartitions is asked once per topic and does not use the remembered topic.
        assertEquals(racksByPartition.size() + 1, describer.numPartitions(topicId));
        assertEquals(2, lookups.get());

        assertEquals(Set.of("other-rack"), describer.racksForPartition(otherTopicId, 0));
        assertEquals(3, lookups.get());
        assertEquals(Set.of("rack0"), describer.racksForPartition(topicId, 1));
        assertEquals(4, lookups.get());
        assertEquals(Set.of(), describer.racksForPartition(Uuid.randomUuid(), 0));
        assertEquals(-1, describer.numPartitions(Uuid.randomUuid()));
        assertEquals(6, lookups.get());
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicDescriberImpl(metadataImage), subscribedTopicMetadata);

        Uuid topicId = Uuid.randomUuid();
        CoordinatorMetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(topicId, "newTopic", 5)
            .addRacks()
            .buildCoordinatorMetadataImage();
        assertNotEquals(new SubscribedTopicDescriberImpl(metadataImage2), subscribedTopicMetadata);
    }
}
