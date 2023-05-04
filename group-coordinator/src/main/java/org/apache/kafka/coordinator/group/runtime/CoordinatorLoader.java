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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;

/**
 * Interface to implement a coordinator loader.
 */
interface CoordinatorLoader<U> {

    /**
     * Loads the coordinator by reading all the records from the TopicPartition
     * and applying them to the Replayable object.
     *
     * @param tp            The TopicPartition to read from.
     * @param replayable    The object to apply records to.
     */
    CompletableFuture<Void> load(
        TopicPartition tp,
        Replayable<U> replayable
    );
}
