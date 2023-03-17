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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An immutable representation of a member subscription.
 */
public class ConsumerGroupMemberSubscription {
    public static ConsumerGroupMemberSubscription EMPTY = new ConsumerGroupMemberSubscription(
        "",
        "",
        -1,
        "",
        "",
        Collections.emptyList(),
        "",
        "",
        Collections.emptyList()
    );

    /**
     * The instance id provided by the member.
     */
    private final String instanceId;

    /**
     * The rack id provided by the member.
     */
    private final String rackId;

    /**
     * The rebalance timeout provided by the member.
     */
    private final int rebalanceTimeoutMs;

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
     * The server side assignor selected by the member.
     */
    private final String serverAssignorName;

    /**
     * The states of the client side assignors of the member.
     */
    private final List<AssignorState> assignorStates;

    public ConsumerGroupMemberSubscription(
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String serverAssignorName,
        List<AssignorState> assignorStates
    ) {
        Objects.requireNonNull(instanceId);
        Objects.requireNonNull(rackId);
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(clientHost);
        Objects.requireNonNull(subscribedTopicNames);
        Objects.requireNonNull(subscribedTopicRegex);
        Objects.requireNonNull(serverAssignorName);
        Objects.requireNonNull(assignorStates);

        this.instanceId = instanceId;
        this.rackId = rackId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = Collections.unmodifiableList(subscribedTopicNames);
        // Sort subscriptions to avoid rebalancing when the order changes.
        this.subscribedTopicNames.sort(String::compareTo);
        this.subscribedTopicRegex = subscribedTopicRegex;
        this.serverAssignorName = serverAssignorName;
        this.assignorStates = Collections.unmodifiableList(assignorStates);
    }

    public String instanceId() {
        return this.instanceId;
    }

    public String rackId() {
        return this.rackId;
    }

    public int rebalanceTimeoutMs() {
        return this.rebalanceTimeoutMs;
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

    public String serverAssignorName() {
        return this.serverAssignorName;
    }

    public List<AssignorState> assignorStates() {
        return this.assignorStates;
    }

    /**
     * Updates the subscription with the provided information and returns
     * an empty Optional if nothing changed or an Optional containing the
     * updated subscriptions.
     */
    public ConsumerGroupMemberSubscription maybeUpdateWith(
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String serverAssignorName,
        List<AssignorState> assignorStates
    ) {
        ConsumerGroupMemberSubscription newConsumerGroupMemberSubscription = new ConsumerGroupMemberSubscription(
            instanceId != null ? instanceId : this.instanceId,
            rackId != null ? rackId : this.rackId,
            rebalanceTimeoutMs,
            clientId,
            clientHost,
            subscribedTopicNames != null ? subscribedTopicNames : this.subscribedTopicNames,
            subscribedTopicRegex != null ? subscribedTopicRegex : this.subscribedTopicRegex,
            serverAssignorName != null ? serverAssignorName : this.serverAssignorName,
            assignorStates != null ? assignorStates : this.assignorStates
        );

        return equals(newConsumerGroupMemberSubscription) ? this : newConsumerGroupMemberSubscription;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroupMemberSubscription that = (ConsumerGroupMemberSubscription) o;

        if (!instanceId.equals(that.instanceId)) return false;
        if (!rackId.equals(that.rackId)) return false;
        if (rebalanceTimeoutMs != that.rebalanceTimeoutMs) return false;
        if (!clientId.equals(that.clientId)) return false;
        if (!clientHost.equals(that.clientHost)) return false;
        if (!subscribedTopicNames.equals(that.subscribedTopicNames)) return false;
        if (!subscribedTopicRegex.equals(that.subscribedTopicRegex)) return false;
        if (!serverAssignorName.equals(that.serverAssignorName)) return false;
        return assignorStates.equals(that.assignorStates);
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();
        result = 31 * result + rackId.hashCode();
        result = 31 * result + rebalanceTimeoutMs;
        result = 31 * result + clientId.hashCode();
        result = 31 * result + clientHost.hashCode();
        result = 31 * result + subscribedTopicNames.hashCode();
        result = 31 * result + subscribedTopicRegex.hashCode();
        result = 31 * result + serverAssignorName.hashCode();
        result = 31 * result + assignorStates.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MemberState(instanceId=" + instanceId +
            ", rackId=" + rackId +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId=" + clientId +
            ", clientHost=" + clientHost +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", subscribedTopicRegex=" + subscribedTopicRegex +
            ", serverAssignorName=" + serverAssignorName +
            ", assignorStates=" + assignorStates +
            ')';
    }

    public static ConsumerGroupMemberSubscription fromRecord(
        ConsumerGroupMemberMetadataValue record
    ) {
        return new ConsumerGroupMemberSubscription(
            record.instanceId(),
            record.rackId(),
            record.rebalanceTimeoutMs(),
            record.clientId(),
            record.clientHost(),
            record.subscribedTopicNames(),
            record.subscribedTopicRegex(),
            record.serverAssignor(),
            record.assignors().stream().map(AssignorState::fromRecord).collect(Collectors.toList())
        );
    }
}