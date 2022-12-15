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

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The state of a member in a consumer group.
 */
public class MemberState {
    /**
     * The member id.
     */
    private final String memberId;

    /**
     * The optional instance id provided by the member.
     */
    private final Optional<String> instanceId;

    /**
     * The optional rack id provided by the member.
     */
    private final Optional<String> rackId;

    /**
     * The client id reported by the member.
     */
    private final String clientId;

    /**
     * The host reported by the member.
     */
    private final String clientHost;

    /**
     * The list of subscriptions (topic names) configured by the member.
     */
    private final List<String> subscribedTopicNames;

    /**
     * The subscription pattern configured by the member,
     */
    private final String subscribedTopicRegex;

    /**
     * The optional server side assignor selected by the member.
     */
    private final Optional<String> serverAssignorName;

    /**
     * The states of the client side assignors of the member.
     */
    private final List<AssignorState> assignorStates;

    public MemberState(
        String memberId,
        Optional<String> instanceId,
        Optional<String> rackId,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        Optional<String> serverAssignorName,
        List<AssignorState> assignorStates
    ) {
        this.memberId = memberId;
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = Collections.unmodifiableList(subscribedTopicNames);
        this.subscribedTopicRegex = subscribedTopicRegex;
        this.serverAssignorName = serverAssignorName;
        this.assignorStates = Collections.unmodifiableList(assignorStates);
    }

    public String memberId() {
        return this.memberId;
    }

    public Optional<String> instanceId() {
        return this.instanceId;
    }

    public Optional<String> rackId() {
        return this.rackId;
    }

    public String clientId() {
        return this.clientId;
    }

    public String clientHost() {
        return this.clientHost;
    }

    public List<String> subscribedTopicNames() {
        return this.subscribedTopicNames;
    }

    public String subscribedTopicRegex() {
        return this.subscribedTopicRegex;
    }

    public Optional<String> serverAssignorName() {
        return this.serverAssignorName;
    }

    public List<AssignorState> assignorStates() {
        return this.assignorStates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberState that = (MemberState) o;
        return Objects.equals(memberId, that.memberId) &&
            Objects.equals(instanceId, that.instanceId) &&
            Objects.equals(rackId, that.rackId) &&
            Objects.equals(clientId, that.clientId) &&
            Objects.equals(clientHost, that.clientHost) &&
            Objects.equals(subscribedTopicNames, that.subscribedTopicNames) &&
            Objects.equals(subscribedTopicRegex, that.subscribedTopicRegex) &&
            Objects.equals(serverAssignorName, that.serverAssignorName) &&
            Objects.equals(assignorStates, that.assignorStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, instanceId, rackId, clientId, clientHost, subscribedTopicNames,
            subscribedTopicRegex, serverAssignorName, assignorStates);
    }

    @Override
    public String toString() {
        return "AssignorState(memberId=" + memberId +
            ", instanceId=" + instanceId +
            ", rackId=" + rackId +
            ", clientId=" + clientId +
            ", clientHost=" + clientHost +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", subscribedTopicRegex=" + subscribedTopicRegex +
            ", serverAssignorName=" + serverAssignorName +
            ", assignorStates=" + assignorStates +
            ")";
    }

    public static MemberState fromRecord(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        return new MemberState(
            key.memberId(),
            value.instanceId() != null ? Optional.of(value.instanceId()) : Optional.empty(),
            value.rackId() != null ? Optional.of(value.rackId()) : Optional.empty(),
            value.clientId(),
            value.clientHost(),
            value.subscribedTopicNames(),
            value.subscribedTopicRegex(),
            Optional.empty(), // TODO
            value.assignors().stream().map(AssignorState::fromRecord).collect(Collectors.toList())
        );
    }
}
