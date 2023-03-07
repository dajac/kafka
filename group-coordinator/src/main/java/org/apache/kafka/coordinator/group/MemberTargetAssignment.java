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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The target assignment for a member in a consumer group. This is the assignment
 * that the member will eventually converge to.
 */
public class MemberTargetAssignment {

    private final Map<Uuid, List<Integer>> assignedPartitions;

    private final VersionedMetadata metadata;

    public MemberTargetAssignment(
        Map<Uuid, List<Integer>> assignedPartitions,
        VersionedMetadata metadata
    ) {
        Objects.requireNonNull(assignedPartitions);
        Objects.requireNonNull(metadata);

        this.assignedPartitions = Collections.unmodifiableMap(assignedPartitions);
        this.metadata = metadata;
    }

    public Map<Uuid, List<Integer>> assignedPartitions() {
        return this.assignedPartitions;
    }

    public VersionedMetadata metadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberTargetAssignment that = (MemberTargetAssignment) o;

        if (!assignedPartitions.equals(that.assignedPartitions)) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = assignedPartitions.hashCode();
        result = 31 * result + metadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MemberTargetAssignment(" +
            "assignedPartitions=" + assignedPartitions +
            ", metadata=" + metadata +
            ')';
    }
}
