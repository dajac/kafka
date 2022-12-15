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

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The Assignor state of a member.
 */
public class AssignorState {
    /**
     * The name of the assignor.
     */
    private final String name;

    /**
     * The reason reported by the assignor.
     */
    private final byte reason;

    /**
     * The minimum metadata version supported by the assignor.
     */
    private final short minimumVersion;

    /**
     * The maximum metadata version supported by the assignor.
     */
    private final short maximumVersion;

    /**
     * The metadata version reported by the assignor.
     */
    private final short metadataVersion;

    /**
     * The metadata raw bytes reported by the assignor.
     */
    private final ByteBuffer metadataBytes;

    public AssignorState(
        String name,
        byte reason,
        short minimumVersion,
        short maximumVersion,
        short metadataVersion,
        ByteBuffer metadataBytes
    ) {
        this.name = name;
        this.reason = reason;
        this.minimumVersion = minimumVersion;
        this.maximumVersion = maximumVersion;
        this.metadataVersion = metadataVersion;
        this.metadataBytes = metadataBytes;
    }

    public String name() {
        return this.name;
    }

    public byte reason() {
        return this.reason;
    }

    public short minimumVersion() {
        return this.minimumVersion;
    }

    public short maximumVersion() {
        return this.maximumVersion;
    }

    public short metadataVersion() {
        return this.metadataVersion;
    }

    public ByteBuffer metadataBytes() {
        return this.metadataBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        AssignorState other = (AssignorState) o;
        return Objects.equals(name, other.name) &&
            reason == other.reason &&
            minimumVersion == other.minimumVersion &&
            maximumVersion == other.maximumVersion &&
            metadataVersion == other.metadataVersion &&
            Objects.equals(metadataBytes, other.metadataBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, reason, minimumVersion, maximumVersion, metadataVersion, metadataBytes);
    }

    @Override
    public String toString() {
        return "AssignorState(name=" + name +
            ", reason=" + reason +
            ", minimumVersion=" + minimumVersion +
            ", maximumVersion=" + maximumVersion +
            ", metadataVersion=" + metadataVersion +
            ", metadataBytes=" + metadataBytes +
            ")";
    }

    public static AssignorState fromRecord(ConsumerGroupMemberMetadataValue.Assignor assignorRecord) {
        return new AssignorState(
            assignorRecord.name(),
            assignorRecord.reason(),
            assignorRecord.minimumVersion(),
            assignorRecord.maximumVersion(),
            assignorRecord.version(),
            ByteBuffer.wrap(assignorRecord.metadata())
        );
    }
}
