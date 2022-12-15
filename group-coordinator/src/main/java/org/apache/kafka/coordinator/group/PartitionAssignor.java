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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public interface PartitionAssignor {

    class AssignmentSpec {
        /**
         * The members keyed by member id.
         */
        Map<String, AssignmentMemberSpec> members;

        /**
         * The topics' metadata keyed by topic id
         */
        Map<Uuid, AssignmentTopicMetadata> topics;

        public AssignmentSpec(
            Map<String, AssignmentMemberSpec> members,
            Map<Uuid, AssignmentTopicMetadata> topics
        ) {
            this.members = members;
            this.topics = topics;
        }

        @Override
        public String toString() {
            return "AssignmentSpec(members=" + members + ", topics=" + topics + ")";
        }
    }

    class AssignmentMemberSpec {
        /**
         * The instance ID if provided.
         */
        Optional<String> instanceId;

        /**
         * The rack ID if provided.
         */
        Optional<String> rackId;

        /**
         * The topics that the member is subscribed to.
         */
        Collection<String> subscribedTopics;

        /**
         * The current target partitions of the member.
         */
        Collection<TopicPartition> targetPartitions;

        public AssignmentMemberSpec(
            Optional<String> instanceId,
            Optional<String> rackId,
            Collection<String> subscribedTopics,
            Collection<TopicPartition> targetPartitions
        ) {
            this.instanceId = instanceId;
            this.rackId = rackId;
            this.subscribedTopics = subscribedTopics;
            this.targetPartitions = targetPartitions;
        }

        @Override
        public String toString() {
            return "AssignmentMemberSpec(instanceId=" + instanceId +
                ", rackId=" + rackId +
                ", subscribedTopics=" + subscribedTopics +
                ", targetPartitions=" + targetPartitions +
                ")";
        }
    }

    class AssignmentTopicMetadata {
        /**
         * The topic name.
         */
        String topicName;

        /**
         * The number of partitions.
         */
        int numPartitions;

        public AssignmentTopicMetadata(
            String topicName,
            int numPartitions
        ) {
            this.topicName = topicName;
            this.numPartitions = numPartitions;
        }

        @Override
        public String toString() {
            return "AssignmentTopicMetadata(topicName=" + topicName + ", numPartitions=" + numPartitions + ")";
        }
    }

    class GroupAssignment {
        /**
         * The member assignments keyed by member id.
         */
        Map<String, MemberAssignment> members;

        public GroupAssignment(Map<String, MemberAssignment> members) {
            this.members = members;
        }

        @Override
        public String toString() {
            return "GroupAssignment(members=" + members + ")";
        }
    }

    class MemberAssignment {
        /**
         * The target partitions assigned to this member.
         */
        Collection<TopicPartition> targetPartitions;

        public MemberAssignment(Collection<TopicPartition> targetPartitions) {
            this.targetPartitions = targetPartitions;
        }

        @Override
        public String toString() {
            return "MemberAssignment(targetPartitions=" + targetPartitions + ")";
        }
    }

    /**
     * Unique name for this assignor.
     */
    String name();

    /**
     * Perform the group assignment given the current members and
     * topic metadata.
     *
     * @param assignmentSpec The assignment spec.
     * @return The new assignment for the group.
     */
    GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException;
}
