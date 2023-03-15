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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MemberCurrentAssignment {

    /**
     * The current epoch of the member.
     */
    private final int epoch;

    /**
     * The current partitions owned by the member.
     */
    private final Map<Uuid, Set<Integer>> partitions;

    /**
     * The current assignor error reported to the member.
     */
    private final byte error;

    /**
     * The current metadata reported to the member.
     */
    private final VersionedMetadata metadata;

    /**
     * The partitions that need to be revoked.
     */
    private final Map<Uuid, Set<Integer>> revoking;

    /**
     * The partitions that need to be assigned.
     */
    private final Map<Uuid, Set<Integer>> assigning;

    public MemberCurrentAssignment(
        int epoch,
        Map<Uuid, Set<Integer>> partitions,
        byte error,
        VersionedMetadata metadata
    ) {
        this(epoch, partitions, null, null, error, metadata);
    }

    public MemberCurrentAssignment(
        int epoch,
        Map<Uuid, Set<Integer>> partitions,
        Map<Uuid, Set<Integer>> revoking,
        Map<Uuid, Set<Integer>> assigning,
        byte error,
        VersionedMetadata metadata
    ) {
        Objects.requireNonNull(partitions);
        Objects.requireNonNull(metadata);

        if (epoch < 0) {
            throw new IllegalStateException("Member epoch should be greater than or equals to zero.");
        }

        if (error < 0) {
            throw new IllegalStateException("Member error should be greater than or equals to zero.");
        }

        this.epoch = epoch;
        this.partitions = Collections.unmodifiableMap(partitions);
        this.error = error;
        this.metadata = metadata;
        this.revoking = revoking;
        this.assigning = assigning;
    }

    public int epoch() {
        return this.epoch;
    }

    public Map<Uuid, Set<Integer>> partitions() {
        return this.partitions;
    }

    public byte error() {
        return this.error;
    }

    public VersionedMetadata metadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberCurrentAssignment that = (MemberCurrentAssignment) o;

        if (epoch != that.epoch) return false;
        if (error != that.error) return false;
        if (!partitions.equals(that.partitions)) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = epoch;
        result = 31 * result + partitions.hashCode();
        result = 31 * result + (int) error;
        result = 31 * result + metadata.hashCode();
        result = 31 * result + (revoking != null ? revoking.hashCode() : 0);
        result = 31 * result + (assigning != null ? assigning.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MemberCurrentAssignment(" +
            "epoch=" + epoch +
            ", partitions=" + partitions +
            ", error=" + error +
            ", metadata=" + metadata +
            ", revoking=" + revoking +
            ", assigning=" + assigning +
            ')';
    }
}
