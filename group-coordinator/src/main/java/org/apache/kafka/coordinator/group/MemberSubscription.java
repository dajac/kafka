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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable representation of a member subscription.
 */
public class MemberSubscription {
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

    public MemberSubscription(
        Optional<String> instanceId,
        Optional<String> rackId,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        Optional<String> serverAssignorName,
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
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = Collections.unmodifiableList(subscribedTopicNames);
        this.subscribedTopicRegex = subscribedTopicRegex;
        this.serverAssignorName = serverAssignorName;
        this.assignorStates = Collections.unmodifiableList(assignorStates);
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

        MemberSubscription that = (MemberSubscription) o;

        if (!instanceId.equals(that.instanceId)) return false;
        if (!rackId.equals(that.rackId)) return false;
        if (!clientId.equals(that.clientId)) return false;
        if (!clientHost.equals(that.clientHost)) return false;
        if (!subscribedTopicNames.equals(that.subscribedTopicNames)) return false;
        if (!subscribedTopicRegex.equals(that.subscribedTopicRegex)) return false;
        if (!serverAssignorName.equals(that.serverAssignorName)) return false;
        return assignorStates.equals(that.assignorStates);
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();;
        result = 31 * result + rackId.hashCode();
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
            ", clientId=" + clientId +
            ", clientHost=" + clientHost +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", subscribedTopicRegex=" + subscribedTopicRegex +
            ", serverAssignorName=" + serverAssignorName +
            ", assignorStates=" + assignorStates +
            ')';
    }
}