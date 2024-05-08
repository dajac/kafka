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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class CopyOnWriteAssignment implements Assignment {
    private final Map<Uuid, Set<Integer>> original;
    private Map<Uuid, Set<Integer>> copy;

    public CopyOnWriteAssignment(Map<Uuid, Set<Integer>> assignment) {
        this.original = assignment;
        this.copy = null;
    }

    @Override
    public Map<Uuid, Set<Integer>> asMap() {
        if (copy != null) {
            return copy;
        } else {
            return original;
        }
    }

    @Override
    public void forEach(BiConsumer<Uuid, Integer> func) {
        for (Map.Entry<Uuid, Set<Integer>> entry : asMap().entrySet()) {
            for (Integer partition : entry.getValue()) {
                func.accept(entry.getKey(), partition);
            }
        }
    }

    @Override
    public void assign(Uuid topicId, int partitionId) {
        if (copy == null) deepCopy();
        copy.computeIfAbsent(topicId, __ -> new HashSet<>()).add(partitionId);
    }

    @Override
    public void unassign(Uuid topicId, int partitionId) {
        if (copy == null) deepCopy();
        Set<Integer> partitions = copy.get(topicId);
        if (partitions != null) {
            partitions.remove(partitionId);
            if (partitions.isEmpty()) {
                copy.remove(topicId);
            }
        }
    }

    @Override
    public int size() {
        return asMap().values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public Set<Uuid> topicIds() {
        return Collections.unmodifiableSet(asMap().keySet());
    }

    @Override
    public Set<Integer> partitions(Uuid topicId) {
        return asMap().getOrDefault(topicId, Collections.emptySet());
    }

    private void deepCopy() {
        copy = new HashMap<>(original.size());
        for (Map.Entry<Uuid, Set<Integer>> entry : original.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
    }
}
