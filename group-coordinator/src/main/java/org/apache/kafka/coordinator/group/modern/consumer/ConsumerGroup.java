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
package org.apache.kafka.coordinator.group.modern.consumer;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.ModernGroupMember;
import org.apache.kafka.coordinator.group.modern.SubscriptionCount;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.METADATA_REFRESH_INTERVAL_MS;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.REGEX_BATCH_REFRESH_INTERVAL_MS;
import static org.apache.kafka.coordinator.group.Utils.assignmentToString;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
import static org.apache.kafka.coordinator.group.Utils.throwIfRegularExpressionIsInvalid;
import static org.apache.kafka.coordinator.group.Utils.toOptional;
import static org.apache.kafka.coordinator.group.Utils.toTopicPartitionMap;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.modern.ModernGroupMember.hasAssignedPartitionsChanged;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.STABLE;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember.subscribedTopicRegexOrNull;

/**
 * A Consumer Group. All the metadata in this class are backed by
 * records in the __consumer_offsets partitions.
 */
public class ConsumerGroup extends ModernGroup<ConsumerGroupMember> {

    public enum ConsumerGroupState {
        EMPTY("Empty"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        ConsumerGroupState(String name) {
            this.name = name;
            this.lowerCaseName = name.toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return name;
        }

        public String toLowerCaseString() {
            return lowerCaseName;
        }
    }

    /**
     * The group state.
     */
    private final TimelineObject<ConsumerGroupState> state;

    /**
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

    /**
     * The number of members supporting each server assignor name.
     */
    private final TimelineHashMap<String, Integer> serverAssignors;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The number of members that use the classic protocol.
     */
    private final TimelineInteger numClassicProtocolMembers;

    /**
     * Map of protocol names to the number of members that use classic protocol and support them.
     */
    private final TimelineHashMap<String, Integer> classicProtocolMembersSupportedProtocols;

    /**
     * The current partition epoch maps each topic-partitions to their current epoch where
     * the epoch is the epoch of their owners. When a member revokes a partition, it removes
     * its epochs from this map. When a member gets a partition, it adds its epochs to this map.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, Integer>> currentPartitionEpoch;

    /**
     * The number of members subscribed to each regular expressions.
     */
    private final TimelineHashMap<String, Integer> subscribedRegularExpressions;

    /**
     * The resolved regular expressions.
     */
    private final TimelineHashMap<String, ResolvedRegularExpression> resolvedRegularExpressions;

    private final Time time;

    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    private final GroupCoordinatorConfig config;

    private final Logger log;

    private final CoordinatorExecutor<CoordinatorRecord> executor;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        String groupId,
        GroupCoordinatorMetricsShard metrics,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        GroupCoordinatorConfig config,
        Time time,
        CoordinatorExecutor<CoordinatorRecord> executor
    ) {
        super(snapshotRegistry, groupId);
        this.log = logContext.logger(ConsumerGroup.class);
        this.state = new TimelineObject<>(snapshotRegistry, EMPTY);
        this.staticMembers = new TimelineHashMap<>(snapshotRegistry, 0);
        this.serverAssignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.timer = Objects.requireNonNull(timer);
        this.time = Objects.requireNonNull(time);
        this.config = Objects.requireNonNull(config);
        this.executor = Objects.requireNonNull(executor);
        this.numClassicProtocolMembers = new TimelineInteger(snapshotRegistry);
        this.classicProtocolMembersSupportedProtocols = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedRegularExpressions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.resolvedRegularExpressions = new TimelineHashMap<>(snapshotRegistry, 0);

    }

    /**
     * @return The group type (Consumer).
     */
    @Override
    public GroupType type() {
        return GroupType.CONSUMER;
    }

    /**
     * @return The group protocol type (consumer).
     */
    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    /**
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return state.get().toString();
    }

    /**
     * @return The current state as a String with given committedOffset.
     */
    public String stateAsString(long committedOffset) {
        return state.get(committedOffset).toString();
    }

    /**
     * @return The current state.
     */
    public ConsumerGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public ConsumerGroupState state(long committedOffset) {
        return state.get(committedOffset);
    }

    /**
     * Sets the number of members using the classic protocol.
     *
     * @param numClassicProtocolMembers The new NumClassicProtocolMembers.
     */
    public void setNumClassicProtocolMembers(int numClassicProtocolMembers) {
        this.numClassicProtocolMembers.set(numClassicProtocolMembers);
    }

    /**
     * Get member id of a static member that matches the given group
     * instance id.
     *
     * @param groupInstanceId The group instance id.
     *
     * @return The member id corresponding to the given instance id or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        if (groupInstanceId == null) return null;
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member
     * is done via the {@link ConsumerGroup#updateMember(ConsumerGroupMember)} method.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     * @throws UnknownMemberIdException when the member does not exist and createIfNotExists is false.
     */
    public ConsumerGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) throws UnknownMemberIdException {
        ConsumerGroupMember member = members.get(memberId);
        if (member != null) return member;

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new ConsumerGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance id.
     *
     * @return The member corresponding to the given instance id or null if it does not exist
     */
    public ConsumerGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    /**
     * Returns true if the static member exists.
     *
     * @param instanceId The instance id.
     *
     * @return A boolean indicating whether the member exists or not.
     */
    public boolean hasStaticMember(String instanceId) {
        if (instanceId == null) return false;
        return staticMembers.containsKey(instanceId);
    }

    /**
     * Returns the target assignment associated to the provided member id if
     * the instance id is null; otherwise returns the target assignment associated
     * to the instance id.
     *
     * @param memberId      The member id.
     * @param instanceId    The instance id.
     *
     * @return The Assignment or EMPTY if it does not exist.
     */
    public Assignment targetAssignment(String memberId, String instanceId) {
        if (instanceId == null) {
            return targetAssignment(memberId);
        } else {
            String previousMemberId = staticMemberId(instanceId);
            if (previousMemberId != null) {
                return targetAssignment(previousMemberId);
            }
        }
        return Assignment.EMPTY;
    }

    @Override
    public void updateMember(ConsumerGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNames(oldMember, newMember);
        maybeUpdateServerAssignors(oldMember, newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        maybeUpdateSubscribedRegularExpression(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
        maybeUpdateNumClassicProtocolMembers(oldMember, newMember);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, newMember);
    }

    /**
     * Updates the member id stored against the instance id if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(ConsumerGroupMember newMember) {
        if (newMember.instanceId() != null) {
            staticMembers.put(newMember.instanceId(), newMember.memberId());
        }
    }

    @Override
    public void removeMember(String memberId) {
        ConsumerGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNames(oldMember, null);
        maybeUpdateServerAssignors(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        maybeUpdateSubscribedRegularExpression(oldMember, null);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
        maybeUpdateNumClassicProtocolMembers(oldMember, null);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, null);
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(ConsumerGroupMember oldMember) {
        if (oldMember != null && oldMember.instanceId() != null) {
            staticMembers.remove(oldMember.instanceId());
        }
    }

    /**
     * Updates the subscription count.
     *
     * @param oldMember             The old member.
     * @param newMember             The new member.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, SubscriptionCount> computeSubscribedTopicNames(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        Map<String, SubscriptionCount> subscribedTopicsNames = super.computeSubscribedTopicNames(oldMember, newMember);
        String oldSubscribedTopicRegex = subscribedTopicRegexOrNull(oldMember);

        if (oldSubscribedTopicRegex != null) {
            String newSubscribedTopicRegex = subscribedTopicRegexOrNull(newMember);

            // If the old member was the last one subscribed to the regex and the new member
            // is not subscribed to it, we must remove it from the subscribed topic names.
            if (!oldSubscribedTopicRegex.equals(newSubscribedTopicRegex) && numSubscribedMembers(oldSubscribedTopicRegex) == 1) {
                resolvedRegularExpression(oldSubscribedTopicRegex).ifPresent(resolvedRegularExpression ->
                    resolvedRegularExpression.topics.forEach(topic -> subscribedTopicsNames.compute(topic, SubscriptionCount::decRegexCount))
                );
            }
        }

        return subscribedTopicsNames;
    }

    /**
     * Computes an updated version of the subscribed regular expressions based on
     * the new/old members.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     * @return An unmodifiable and updated copy of the map.
     */
    public Map<String, Integer> computeSubscribedRegularExpressions(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        String oldRegex = subscribedTopicRegexOrNull(oldMember);
        String newRegex = subscribedTopicRegexOrNull(newMember);

        if (!Objects.equals(oldRegex, newRegex)) {
            Map<String, Integer> newSubscribedRegularExpressions = new HashMap<>(subscribedRegularExpressions);
            if (oldRegex != null) {
                newSubscribedRegularExpressions.compute(oldRegex, Utils::decValue);
            }
            if (newRegex != null) {
                newSubscribedRegularExpressions.compute(newRegex, Utils::incValue);
            }
            return Collections.unmodifiableMap(newSubscribedRegularExpressions);
        } else {
            return Collections.unmodifiableMap(subscribedRegularExpressions);
        }
    }

    /**
     * Computes an updated copy of the subscribed topic names without the provided
     * removed members and removed regular expressions.
     *
     * @param removedMembers    The set of removed members.
     * @param removedRegexes    The set of removed regular expressions.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, SubscriptionCount> computeSubscribedTopicNamesWithoutDeletedMembers(
        Set<ConsumerGroupMember> removedMembers,
        Set<String> removedRegexes
    ) {
        Map<String, SubscriptionCount> subscribedTopicsNames = super.computeSubscribedTopicNames(removedMembers);

        removedRegexes.forEach(regex ->
            resolvedRegularExpression(regex).ifPresent(resolvedRegularExpression ->
                resolvedRegularExpression.topics.forEach(topic ->
                    subscribedTopicsNames.compute(topic, SubscriptionCount::decRegexCount)
                )
            )
        );

        return subscribedTopicsNames;
    }

    /**
     * Update the resolved regular expression.
     *
     * @param regex                         The regular expression.
     * @param newResolvedRegularExpression  The regular expression's metadata.
     */
    public void updateResolvedRegularExpression(
        String regex,
        ResolvedRegularExpression newResolvedRegularExpression
    ) {
        removeResolvedRegularExpression(regex);
        if (newResolvedRegularExpression != null) {
            resolvedRegularExpressions.put(regex, newResolvedRegularExpression);
            newResolvedRegularExpression.topics.forEach(topicName -> subscribedTopicNames.compute(topicName, SubscriptionCount::incRegexCount));
        }
    }

    /**
     * Remove the resolved regular expression.
     *
     * @param regex The regular expression.
     */
    public void removeResolvedRegularExpression(String regex) {
        ResolvedRegularExpression oldResolvedRegularExpression = resolvedRegularExpressions.remove(regex);
        if (oldResolvedRegularExpression != null) {
            oldResolvedRegularExpression.topics.forEach(topicName -> subscribedTopicNames.compute(topicName, SubscriptionCount::decRegexCount));
        }
    }

    /**
     * @return The last time resolved regular expressions were refreshed or Long.MIN_VALUE if
     * there are no resolved regular expression. Note that we use the timestamp of the first
     * entry as a proxy for all of them. They are always resolved together.
     */
    public long lastResolvedRegularExpressionRefreshTimeMs() {
        Iterator<ResolvedRegularExpression> iterator = resolvedRegularExpressions.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next().timestamp;
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * @return The version of the regular expressions or Zero if there are no resolved regular expression.
     */
    public long lastResolvedRegularExpressionVersion() {
        Iterator<ResolvedRegularExpression> iterator = resolvedRegularExpressions.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next().version;
        } else {
            return 0L;
        }
    }

    /**
     * Return an optional containing the resolved regular expression corresponding to the provided regex
     * or an empty optional.
     *
     * @param regex The regular expression.
     * @return The optional containing the resolved regular expression or an empty optional.
     */
    public Optional<ResolvedRegularExpression> resolvedRegularExpression(String regex) {
        return Optional.ofNullable(resolvedRegularExpressions.get(regex));
    }

    /**
     * @return The number of resolved regular expressions.
     */
    public int numResolvedRegularExpressions() {
        return resolvedRegularExpressions.size();
    }

    /**
     * @return The number of members subscribed to the provided regex.
     */
    public int numSubscribedMembers(String regex) {
        return subscribedRegularExpressions.getOrDefault(regex, 0);
    }

    /**
     * @return An immutable map containing all the subscribed regular expressions
     *         with the subscribers counts.
     */
    public Map<String, Integer> subscribedRegularExpressions() {
        return Collections.unmodifiableMap(subscribedRegularExpressions);
    }

    /**
     * @return The number of members that use the classic protocol.
     */
    public int numClassicProtocolMembers() {
        return numClassicProtocolMembers.get();
    }

    /**
     * @return The map of the protocol name and the number of members using the classic protocol that support it.
     */
    public Map<String, Integer> classicMembersSupportedProtocols() {
        return Collections.unmodifiableMap(classicProtocolMembersSupportedProtocols);
    }

    /**
     * @return An immutable Map containing all the static members keyed by instance id.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * @return An immutable Map containing all the resolved regular expressions.
     */
    public Map<String, ResolvedRegularExpression> resolvedRegularExpressions() {
        return Collections.unmodifiableMap(resolvedRegularExpressions);
    }

    /**
     * Returns the current epoch of a partition or -1 if the partition
     * does not have one.
     *
     * @param topicId       The topic id.
     * @param partitionId   The partition id.
     *
     * @return The epoch or -1.
     */
    public int currentPartitionEpoch(
        Uuid topicId,
        int partitionId
    ) {
        Map<Integer, Integer> partitions = currentPartitionEpoch.get(topicId);
        if (partitions == null) {
            return -1;
        } else {
            return partitions.getOrDefault(partitionId, -1);
        }
    }

    /**
     * Compute the preferred (server side) assignor for the group while
     * taking into account the updated member. The computation relies
     * on {{@link ConsumerGroup#serverAssignors}} persisted structure
     * but it does not update it.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     *
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> computePreferredServerAssignor(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Copy the current count and update it.
        Map<String, Integer> counts = new HashMap<>(this.serverAssignors);
        maybeUpdateServerAssignors(counts, oldMember, newMember);

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * @return The preferred assignor for the group.
     */
    public Optional<String> preferredServerAssignor() {
        return preferredServerAssignor(Long.MAX_VALUE);
    }

    /**
     * @return The preferred assignor for the group with given offset.
     */
    public Optional<String> preferredServerAssignor(long committedOffset) {
        return serverAssignors.entrySet(committedOffset).stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId          The member id.
     * @param groupInstanceId   The group instance id.
     * @param memberEpoch       The member epoch.
     * @param isTransactional   Whether the offset commit is transactional or not. It has no
     *                          impact when a consumer group is used.
     * @param apiVersion        The api version.
     * @throws UnknownMemberIdException     If the member is not found.
     * @throws StaleMemberEpochException    If the member uses the consumer protocol and the provided
     *                                      member epoch doesn't match the actual member epoch.
     * @throws IllegalGenerationException   If the member uses the classic protocol and the provided
     *                                      generation id is not equal to the member epoch.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional,
        int apiVersion
    ) throws UnknownMemberIdException, StaleMemberEpochException, IllegalGenerationException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) return;

        // The TxnOffsetCommit API does not require the member id, the generation id and the group instance id fields.
        // Hence, they are only validated if any of them is provided
        if (isTransactional && memberEpoch == JoinGroupRequest.UNKNOWN_GENERATION_ID &&
            memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) && groupInstanceId == null)
            return;

        final ConsumerGroupMember member = getOrMaybeCreateMember(memberId, false);

        // If the commit is not transactional and the member uses the new consumer protocol (KIP-848),
        // the member should be using the OffsetCommit API version >= 9.
        if (!isTransactional && !member.useClassicProtocol() && apiVersion < 9) {
            throw new UnsupportedVersionException("OffsetCommit version 9 or above must be used " +
                "by members using the modern group protocol");
        }

        validateMemberEpoch(memberEpoch, member.memberEpoch(), member.useClassicProtocol());
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id for consumer groups.
     * @param memberEpoch           The member epoch for consumer groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     * @throws UnknownMemberIdException     If the member is not found.
     * @throws StaleMemberEpochException    If the member uses the consumer protocol and the provided
     *                                      member epoch doesn't match the actual member epoch.
     * @throws IllegalGenerationException   If the member uses the classic protocol and the provided
     *                                      generation id is not equal to the member epoch.
     */
    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws UnknownMemberIdException, StaleMemberEpochException, IllegalGenerationException {
        // When the member id is null and the member epoch is -1, the request either comes
        // from the admin client or from a client which does not provide them. In this case,
        // the fetch request is accepted.
        if (memberId == null && memberEpoch < 0) return;

        final ConsumerGroupMember member = members.get(memberId, lastCommittedOffset);
        if (member == null) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                memberId, groupId));
        }
        validateMemberEpoch(memberEpoch, member.memberEpoch(), member.useClassicProtocol());
    }

    /**
     * Validates the OffsetDelete request.
     */
    @Override
    public void validateOffsetDelete() {
        // Do nothing.
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != ConsumerGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId))
        );

        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId))
        );
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId));

        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId))
        );

        resolvedRegularExpressions.keySet().forEach(regex ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, regex))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId));
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     * If the removed member is the leaving member, create its tombstone with
     * the joining member id.
     *
     * @param records           The list of records.
     * @param leavingMemberId   The leaving member id.
     * @param joiningMemberId   The joining member id.
     */
    public void createGroupTombstoneRecordsWithReplacedMember(
        List<CoordinatorRecord> records,
        String leavingMemberId,
        String joiningMemberId
    ) {
        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, removedMemberId));
        });

        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, removedMemberId));
        });
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId));

        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, removedMemberId));
        });

        resolvedRegularExpressions.keySet().forEach(regex ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, regex))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId));
    }

    @Override
    public boolean isEmpty() {
        return state() == ConsumerGroupState.EMPTY;
    }

    /**
     * See {@link org.apache.kafka.coordinator.group.OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    @Override
    public Optional<OffsetExpirationCondition> offsetExpirationCondition() {
        return Optional.of(new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs));
    }

    @Override
    public boolean isInStates(Set<String> statesFilter, long committedOffset) {
        return statesFilter.contains(state.get(committedOffset).toLowerCaseString());
    }

    /**
     * Throws an exception if the received member epoch does not match the expected member epoch.
     *
     * @param receivedMemberEpoch   The received member epoch or generation id.
     * @param expectedMemberEpoch   The expected member epoch.
     * @param useClassicProtocol    The boolean indicating whether the checked member uses the classic protocol.
     * @throws StaleMemberEpochException    if the member with unmatched member epoch uses the consumer protocol.
     * @throws IllegalGenerationException   if the member with unmatched generation id uses the classic protocol.
     */
    private void validateMemberEpoch(
        int receivedMemberEpoch,
        int expectedMemberEpoch,
        boolean useClassicProtocol
    ) throws StaleMemberEpochException, IllegalGenerationException {
        if (receivedMemberEpoch != expectedMemberEpoch) {
            if (useClassicProtocol) {
                throw new IllegalGenerationException(String.format("The received generation id %d does not match " +
                    "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
            } else {
                throw new StaleMemberEpochException(String.format("The received member epoch %d does not match "
                    + "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
            }
        }
    }

    /**
     * Computes the subscription type based on the provided information.
     *
     * @param subscribedRegularExpressions  The subscribed regular expression count.
     * @param subscribedTopicNames          The subscribed topic name count.
     * @param numberOfMembers               The number of members in the group.
     *
     * @return The subscription type.
     */
    public static SubscriptionType subscriptionType(
        Map<String, Integer> subscribedRegularExpressions,
        Map<String, SubscriptionCount> subscribedTopicNames,
        int numberOfMembers
    ) {
        if (subscribedRegularExpressions.isEmpty()) {
            // If the members do not use regular expressions, the subscription is
            // considered as homogeneous if all the members are subscribed to the
            // same topics. Otherwise, it is considered as heterogeneous.
            for (SubscriptionCount subscriberCount : subscribedTopicNames.values()) {
                if (subscriberCount.byNameCount != numberOfMembers) {
                    return HETEROGENEOUS;
                }
            }
            return HOMOGENEOUS;
        } else {
            int count = subscribedRegularExpressions.values().iterator().next();
            if (count == numberOfMembers) {
                // If all the members are subscribed to a single regular expressions
                // and none of them are subscribed to topic names, the subscription
                // is considered as homogeneous. If some members are subscribed to
                // topic names too, the subscription is considered as heterogeneous.
                for (SubscriptionCount subscriberCount : subscribedTopicNames.values()) {
                    if (subscriberCount.byRegexCount != 1 || subscriberCount.byNameCount > 0) {
                        return HETEROGENEOUS;
                    }
                }
                return HOMOGENEOUS;
            } else {
                // The subscription is considered as heterogeneous because
                // there is a mix of regular expressions.
                return SubscriptionType.HETEROGENEOUS;
            }
        }
    }

    @Override
    protected void maybeUpdateGroupSubscriptionType() {
        subscriptionType.set(subscriptionType(
            subscribedRegularExpressions,
            subscribedTopicNames,
            members.size()
        ));
    }

    @Override
    protected void maybeUpdateGroupState() {
        ConsumerGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (ModernGroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
    }

    /**
     * Updates the server assignors count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateServerAssignors(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeUpdateServerAssignors(serverAssignors, oldMember, newMember);
    }

    /**
     * Updates the server assignors count.
     *
     * @param serverAssignorCount   The count to update.
     * @param oldMember             The old member.
     * @param newMember             The new member.
     */
    private static void maybeUpdateServerAssignors(
        Map<String, Integer> serverAssignorCount,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, Utils::decValue)
            );
        }
        if (newMember != null) {
            newMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, Utils::incValue)
            );
        }
    }

    /**
     * Updates the number of the members that use the regular expression.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateSubscribedRegularExpression(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Decrement the count of the old regex.
        if (oldMember != null && oldMember.subscribedTopicRegex() != null && !oldMember.subscribedTopicRegex().isEmpty()) {
            subscribedRegularExpressions.compute(oldMember.subscribedTopicRegex(), Utils::decValue);
        }
        // Increment the count of the new regex.
        if (newMember != null && newMember.subscribedTopicRegex() != null && !newMember.subscribedTopicRegex().isEmpty()) {
            subscribedRegularExpressions.compute(newMember.subscribedTopicRegex(), Utils::incValue);
        }
    }

    /**
     * Updates the number of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateNumClassicProtocolMembers(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        int delta = 0;
        if (oldMember != null && oldMember.useClassicProtocol()) {
            delta--;
        }
        if (newMember != null && newMember.useClassicProtocol()) {
            delta++;
        }
        setNumClassicProtocolMembers(numClassicProtocolMembers() + delta);
    }

    /**
     * Updates the supported protocol count of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateClassicProtocolMembersSupportedProtocols(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), Utils::decValue)
                )
            );
        }
        if (newMember != null) {
            newMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), Utils::incValue)
                )
            );
        }
    }

    /**
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeRemovePartitionEpoch(oldMember);
        addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
        addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
    }

    /**
     * Removes the partition epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemovePartitionEpoch(
        ConsumerGroupMember oldMember
    ) {
        if (oldMember != null) {
            removePartitionEpochs(oldMember.assignedPartitions(), oldMember.memberEpoch());
            removePartitionEpochs(oldMember.partitionsPendingRevocation(), oldMember.memberEpoch());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedEpoch The expected epoch.
     * @throws IllegalStateException if the epoch does not match the expected one.
     * package-private for testing.
     */
    void removePartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        Integer prevValue = partitionsOrNull.remove(partitionId);
                        if (prevValue != expectedEpoch) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the epoch %d from %s-%s because the partition is " +
                                    "still owned at a different epoch %d", expectedEpoch, topicId, partitionId, prevValue));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the epoch %d from %s because it does not have any epoch",
                            expectedEpoch, topicId));
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     * @throws IllegalStateException if the partition already has an epoch assigned.
     * package-private for testing.
     */
    void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    Integer prevValue = partitionsOrNull.put(partitionId, epoch);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the epoch of %s-%s to %d because the partition is " +
                                "still owned at epoch %d", topicId, partitionId, epoch, prevValue));
                    }
                }
                return partitionsOrNull;
            });
        });
    }

    public ConsumerGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset,
        String defaultAssignor,
        TopicsImage topicsImage
    ) {
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignorName(preferredServerAssignor(committedOffset).orElse(defaultAssignor))
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                entry.getValue().asConsumerGroupDescribeMember(
                    targetAssignment.get(entry.getValue().memberId(), committedOffset),
                    topicsImage
                )
            )
        );
        return describedGroup;
    }

    /**
     * Create a new consumer group according to the given classic group.
     *
     * @param snapshotRegistry  The SnapshotRegistry.
     * @param metrics           The GroupCoordinatorMetricsShard.
     * @param classicGroup      The converted classic group.
     * @param topicsImage       The TopicsImage for topic id and topic name conversion.
     * @return  The created ConsumerGroup.
     *
     * @throws SchemaException if any member's subscription or assignment cannot be deserialized.
     * @throws UnsupportedVersionException if userData from a custom assignor would be lost.
     */
    public static ConsumerGroup fromClassicGroup(
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        ClassicGroup classicGroup,
        TopicsImage topicsImage
    ) {
        String groupId = classicGroup.groupId();
        ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
        consumerGroup.setGroupEpoch(classicGroup.generationId());
        consumerGroup.setTargetAssignmentEpoch(classicGroup.generationId());

        classicGroup.allMembers().forEach(classicGroupMember -> {
            // The assigned partition can be empty if the member just joined and has never synced.
            // We should accept the empty assignment.
            Map<Uuid, Set<Integer>> assignedPartitions;
            if (Arrays.equals(classicGroupMember.assignment(), EMPTY_ASSIGNMENT)) {
                assignedPartitions = Map.of();
            } else {
                ConsumerProtocolAssignment assignment = ConsumerProtocol.deserializeConsumerProtocolAssignment(
                    ByteBuffer.wrap(classicGroupMember.assignment())
                );
                if (assignment.userData() != null && assignment.userData().hasRemaining()) {
                    throw new UnsupportedVersionException("userData from a custom assignor would be lost");
                }
                assignedPartitions = toTopicPartitionMap(assignment, topicsImage);
            }

            // Every member is guaranteed to have metadata set when it joins,
            // so we don't check for empty subscription here.
            ConsumerProtocolSubscription subscription = ConsumerProtocol.deserializeConsumerProtocolSubscription(
                ByteBuffer.wrap(classicGroupMember.metadata(classicGroup.protocolName().get()))
            );

            // The target assignment and the assigned partitions of each member are set based on the last
            // assignment of the classic group. All the members are put in the Stable state. If the classic
            // group was in Preparing Rebalance or Completing Rebalance states, the classic members are
            // asked to rejoin the group to re-trigger a rebalance or collect their assignments.
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(classicGroupMember.memberId())
                .setMemberEpoch(classicGroup.generationId())
                .setState(MemberState.STABLE)
                .setPreviousMemberEpoch(classicGroup.generationId())
                .setInstanceId(classicGroupMember.groupInstanceId().orElse(null))
                .setRackId(toOptional(subscription.rackId()).orElse(null))
                .setRebalanceTimeoutMs(classicGroupMember.rebalanceTimeoutMs())
                .setClientId(classicGroupMember.clientId())
                .setClientHost(classicGroupMember.clientHost())
                .setSubscribedTopicNames(subscription.topics())
                .setAssignedPartitions(assignedPartitions)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(classicGroupMember.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                            classicGroupMember.supportedProtocols()
                        ))
                )
                .build();
            consumerGroup.updateTargetAssignment(newMember.memberId(), new Assignment(assignedPartitions));
            consumerGroup.updateMember(newMember);
        });

        return consumerGroup;
    }

    /**
     * Populate the record list with the records needed to create the given consumer group.
     *
     * @param records The list to which the new records are added.
     */
    public void createConsumerGroupRecords(
        List<CoordinatorRecord> records
    ) {
        members().forEach((__, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId(), consumerGroupMember))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId(), groupEpoch()));

        members().forEach((consumerGroupMemberId, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
                groupId(),
                consumerGroupMemberId,
                targetAssignment(consumerGroupMemberId).partitions()
            ))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId(), groupEpoch()));

        members().forEach((__, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId(), consumerGroupMember))
        );
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members that use the
     * classic protocol.
     *
     * @param memberProtocolType  The member protocol type.
     * @param memberProtocols     The set of protocol names.
     *
     * @return A boolean based on the condition mentioned above.
     */
    public boolean supportsClassicProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (ConsumerProtocol.PROTOCOL_TYPE.equals(memberProtocolType)) {
            if (isEmpty()) {
                return !memberProtocols.isEmpty();
            } else {
                return memberProtocols.stream().anyMatch(
                    name -> classicProtocolMembersSupportedProtocols.getOrDefault(name, 0) == numClassicProtocolMembers()
                );
            }
        }
        return false;
    }

    /**
     * Checks whether all the members use the classic protocol except the given member.
     *
     * @param member The member to remove.
     * @return A boolean indicating whether all the members use the classic protocol.
     */
    public boolean allMembersUseClassicProtocolExcept(ConsumerGroupMember member) {
        return numClassicProtocolMembers() == members().size() - 1 && !member.useClassicProtocol();
    }

    /**
     * Checks whether all the members use the classic protocol except the given members.
     *
     * @param members The members to remove.
     * @return A boolean indicating whether all the members use the classic protocol.
     */
    public boolean allMembersUseClassicProtocolExcept(Set<ConsumerGroupMember> members) {
        int numExcludedClassicProtocolMembers = 0;
        for (ConsumerGroupMember member : members) {
            if (member.useClassicProtocol()) {
                numExcludedClassicProtocolMembers++;
            }
        }
        return numClassicProtocolMembers() - numExcludedClassicProtocolMembers == members().size() - members.size();
    }

    /**
     * Checks whether the member has any unreleased partition.
     *
     * @param member The member to check.
     * @return A boolean indicating whether the member has partitions in the target
     *         assignment that hasn't been revoked by other members.
     */
    public boolean waitingOnUnreleasedPartition(ConsumerGroupMember member) {
        if (member.state() == MemberState.UNRELEASED_PARTITIONS) {
            for (Map.Entry<Uuid, Set<Integer>> entry : targetAssignment().get(member.memberId()).partitions().entrySet()) {
                Uuid topicId = entry.getKey();
                Set<Integer> assignedPartitions = member.assignedPartitions().getOrDefault(topicId, Set.of());

                for (int partition : entry.getValue()) {
                    if (!assignedPartitions.contains(partition) && currentPartitionEpoch(topicId, partition) != -1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Checks whether the consumer group can accept a new member or not based on the
     * max group size defined.
     *
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfConsumerGroupIsFull(
        String memberId
    ) throws GroupMaxSizeReachedException {
        // If the consumer group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the consumer group.
        if (numMembers() >= config.consumerGroupMaxSize() && (memberId.isEmpty() || !hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The consumer group has reached its maximum capacity of "
                + config.consumerGroupMaxSize() + " members.");
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnreleasedInstanceIdException if the instance id received in the request is still in use by an existing static member.
     */
    private void throwIfInstanceIdIsUnreleased(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId) {
        if (member.memberEpoch() != LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // The new member can't join.
            log.info("[GroupId {}] Static member {} with instance id {} cannot join the group because the instance id is" +
                " owned by member {}.", groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.UNRELEASED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " cannot join the group because the instance id is owned by " + member.memberId() + " member.");
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The consumer group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedTopicPartitions  The owned partitions.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead of or behind the epoch known
     *                                    by this coordinator.
     */
    private static void throwIfConsumerGroupMemberEpochIsInvalid(
        ConsumerGroupMember member,
        int receivedMemberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The consumer group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch() || !isSubset(ownedTopicPartitions, member.assignedPartitions())) {
                throw new FencedMemberEpochException("The consumer group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param staticMember          The static member in the group.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnknownMemberIdException if no static member exists in the group against the provided instance id.
     */
    private void throwIfStaticMemberIsUnknown(ConsumerGroupMember staticMember, String receivedInstanceId) {
        if (staticMember == null) {
            throw Errors.UNKNOWN_MEMBER_ID.exception("Instance id " + receivedInstanceId + " is unknown.");
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws FencedInstanceIdException if the instance id provided is fenced because of another static member.
     */
    private void throwIfInstanceIdIsFenced(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId) {
        if (!member.memberId().equals(receivedMemberId)) {
            log.info("[GroupId {}] Static member {} with instance id {} is fenced by existing member {}.",
                groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.FENCED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " was fenced by member " + member.memberId() + ".");
        }
    }

    /**
     * Verifies that the partitions currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the consumer
     * only owns partitions which are in the assigned partitions. It does not match if
     * it owns any other partitions.
     *
     * @param ownedTopicPartitions  The partitions provided by the consumer in the request.
     * @param target                The partitions that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private static boolean isSubset(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        Map<Uuid, Set<Integer>> target
    ) {
        if (ownedTopicPartitions == null) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = target.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Gets or subscribes a new dynamic consumer group member.
     *
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     *
     * @return The existing consumer group member or a new one.
     */
    private ConsumerGroupMember getOrMaybeSubscribeDynamicConsumerGroupMember(
        String memberId,
        int memberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol
    ) {
        ConsumerGroupMember member = getOrMaybeCreateMember(memberId, createIfNotExists);
        if (!useClassicProtocol) {
            throwIfConsumerGroupMemberEpochIsInvalid(member, memberEpoch, ownedTopicPartitions);
        }
        if (createIfNotExists) {
            log.info("[GroupId {}] Member {} joins the consumer group using the {} protocol.",
                groupId(), memberId, useClassicProtocol ? "classic" : "consumer");
        }
        return member;
    }

    /**
     * Write tombstones for the member. The order matters here.
     *
     * @param records       The list of records to append the member assignment tombstone records.
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void removeMember(List<CoordinatorRecord> records, String groupId, String memberId) {
        records.add(newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId));
    }

    /**
     * Write records to replace the old member by the new member.
     *
     * @param records   The list of records to append to.
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void replaceMember(
        List<CoordinatorRecord> records,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        String groupId = groupId();

        // Remove the member without canceling its timers in case the change is reverted. If the
        // change is not reverted, the group validation will fail and the timer will do nothing.
        removeMember(records, groupId, oldMember.memberId());

        // Generate records.
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(
            groupId,
            newMember
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
            groupId,
            newMember.memberId(),
            targetAssignment(oldMember.memberId()).partitions()
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(
            groupId,
            newMember
        ));
    }

    /**
     * Gets or subscribes a static consumer group member. This method also replaces the
     * previous static member if allowed.
     *
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param instanceId            The instance id.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     * @param records               The list to accumulate records created to replace
     *                              the previous static member.
     *
     * @return The existing consumer group member or a new one.
     */
    private ConsumerGroupMember getOrMaybeSubscribeStaticConsumerGroupMember(
        String memberId,
        int memberEpoch,
        String instanceId,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol,
        List<CoordinatorRecord> records
    ) {
        ConsumerGroupMember existingStaticMemberOrNull = staticMember(instanceId);

        if (createIfNotExists) {
            // A new static member joins or the existing static member rejoins.
            if (existingStaticMemberOrNull == null) {
                // New static member.
                ConsumerGroupMember newMember = getOrMaybeCreateMember(memberId, true);
                log.info("[GroupId {}] Static member {} with instance id {} joins the consumer group using the {} protocol.",
                    groupId(), memberId, instanceId, useClassicProtocol ? "classic" : "consumer");
                return newMember;
            } else {
                if (!useClassicProtocol && !existingStaticMemberOrNull.useClassicProtocol()) {
                    // If both the rejoining static member and the existing static member use the consumer
                    // protocol, replace the previous instance iff the previous member had sent a leave group.
                    throwIfInstanceIdIsUnreleased(existingStaticMemberOrNull, groupId(), memberId, instanceId);
                }

                // Copy the member but with its new member id.
                ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(existingStaticMemberOrNull, memberId)
                    .setMemberEpoch(0)
                    .setPreviousMemberEpoch(0)
                    .build();

                // Generate the records to replace the member. We don't care about the regular expression
                // here because it is taken care of later after the static membership replacement.
                replaceMember(records, existingStaticMemberOrNull, newMember);

                log.info("[GroupId {}] Static member with instance id {} re-joins the consumer group " +
                        "using the {} protocol. Created a new member {} to replace the existing member {}.",
                    groupId(), instanceId, useClassicProtocol ? "classic" : "consumer", memberId, existingStaticMemberOrNull.memberId());

                return newMember;
            }
        } else {
            throwIfStaticMemberIsUnknown(existingStaticMemberOrNull, instanceId);
            throwIfInstanceIdIsFenced(existingStaticMemberOrNull, groupId(), memberId, instanceId);
            if (!useClassicProtocol) {
                throwIfConsumerGroupMemberEpochIsInvalid(existingStaticMemberOrNull, memberEpoch, ownedTopicPartitions);
            }
            return existingStaticMemberOrNull;
        }
    }

    /**
     * Creates the member subscription record if the updatedMember is different from
     * the old member. Returns true if the subscribedTopicNames has changed.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the updatedMember has a different
     *         subscribedTopicNames from the old member.
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    private boolean hasMemberSubscriptionChanged(
        String groupId,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) throws InvalidRegularExpression {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupMemberSubscriptionRecord(groupId, updatedMember));
            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.debug("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                return true;
            }
        }
        return false;
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    /**
     * Resolves the provided regular expressions. Note that this static method is executed
     * as an asynchronous task in the executor. Hence, it should not access any state from
     * the manager.
     *
     * @param context       The request context.
     * @param groupId       The group id.
     * @param log           The log instance.
     * @param time          The time instance.
     * @param image         The metadata image to use for listing the topics.
     * @param authorizerPlugin    The authorizer.
     * @param regexes       The list of regular expressions that must be resolved.
     * @return The list of resolved regular expressions.
     *
     * public for benchmarks.
     */
    public static Map<String, ResolvedRegularExpression> refreshRegularExpressions(
        AuthorizableRequestContext context,
        String groupId,
        Logger log,
        Time time,
        MetadataImage image,
        Optional<Plugin<Authorizer>> authorizerPlugin,
        Set<String> regexes
    ) {
        long startTimeMs = time.milliseconds();
        log.debug("[GroupId {}] Refreshing regular expressions: {}", groupId, regexes);

        Map<String, Set<String>> resolvedRegexes = new HashMap<>(regexes.size());
        List<Pattern> compiledRegexes = new ArrayList<>(regexes.size());
        for (String regex : regexes) {
            resolvedRegexes.put(regex, new HashSet<>());
            try {
                compiledRegexes.add(Pattern.compile(regex));
            } catch (PatternSyntaxException ex) {
                // This should not happen because the regular expressions are validated
                // when received from the members. If for some reason, it would
                // happen, we log it and ignore it.
                log.error("[GroupId {}] Couldn't parse regular expression '{}' due to `{}`. Ignoring it.",
                    groupId, regex, ex.getDescription());
            }
        }

        for (String topicName : image.topics().topicsByName().keySet()) {
            for (Pattern regex : compiledRegexes) {
                if (regex.matcher(topicName).matches()) {
                    resolvedRegexes.get(regex.pattern()).add(topicName);
                }
            }
        }

        filterTopicDescribeAuthorizedTopics(
            context,
            authorizerPlugin,
            resolvedRegexes
        );

        long version = image.provenance().lastContainedOffset();
        Map<String, ResolvedRegularExpression> result = new HashMap<>(resolvedRegexes.size());
        for (Map.Entry<String, Set<String>> resolvedRegex : resolvedRegexes.entrySet()) {
            result.put(
                resolvedRegex.getKey(),
                new ResolvedRegularExpression(resolvedRegex.getValue(), version, startTimeMs)
            );
        }

        log.info("[GroupId {}] Scanned {} topics to refresh regular expressions {} in {}ms.",
            groupId, image.topics().topicsByName().size(), resolvedRegexes.keySet(),
            time.milliseconds() - startTimeMs);

        return result;
    }

    /**
     * This method filters the topics in the resolved regexes
     * that the member is authorized to describe.
     *
     * @param context           The request context.
     * @param authorizerPlugin  The authorizer.
     * @param resolvedRegexes   The map of the regex pattern and its set of matched topics.
     */
    private static void filterTopicDescribeAuthorizedTopics(
        AuthorizableRequestContext context,
        Optional<Plugin<Authorizer>> authorizerPlugin,
        Map<String, Set<String>> resolvedRegexes
    ) {
        if (authorizerPlugin.isEmpty()) return;

        Map<String, Integer> topicNameCount = new HashMap<>();
        resolvedRegexes.values().forEach(topicNames ->
            topicNames.forEach(topicName ->
                topicNameCount.compute(topicName, Utils::incValue)
            )
        );

        List<Action> actions = topicNameCount.entrySet().stream().map(entry -> {
            ResourcePattern resource = new ResourcePattern(TOPIC, entry.getKey(), LITERAL);
            return new Action(DESCRIBE, resource, entry.getValue(), true, false);
        }).collect(Collectors.toList());

        List<AuthorizationResult> authorizationResults = authorizerPlugin.get().get().authorize(context, actions);
        Set<String> deniedTopics = new HashSet<>();
        IntStream.range(0, actions.size()).forEach(i -> {
            if (authorizationResults.get(i) == AuthorizationResult.DENIED) {
                String deniedTopic = actions.get(i).resourcePattern().name();
                deniedTopics.add(deniedTopic);
            }
        });

        resolvedRegexes.forEach((__, topicNames) -> topicNames.removeAll(deniedTopics));
    }

    /**
     * Handle the result of the asynchronous tasks which resolves the regular expressions.
     *
     * @param groupId                       The group id.
     * @param memberId                      The member id.
     * @param resolvedRegularExpressions    The resolved regular expressions.
     * @param exception                     The exception if the resolution failed.
     * @return A CoordinatorResult containing the records to mutate the group state.
     */
    private CoordinatorResult<Void, CoordinatorRecord> handleRegularExpressionsResult(
        String groupId,
        String memberId,
        Map<String, ResolvedRegularExpression> resolvedRegularExpressions,
        Throwable exception
    ) {
        if (exception != null) {
            log.error("[GroupId {}] Couldn't update regular expression due to: {}",
                groupId, exception.getMessage());
            return new CoordinatorResult<>(List.of());
        }

        if (log.isDebugEnabled()) {
            log.debug("[GroupId {}] Received updated regular expressions based on the context of member {}: {}.",
                groupId, memberId, resolvedRegularExpressions);
        }

        List<CoordinatorRecord> records = new ArrayList<>();
        try {
            // FIXME
            ConsumerGroup group = consumerGroup(groupId);
            Map<String, SubscriptionCount> subscribedTopicNames = new HashMap<>(group.subscribedTopicNames());

            boolean bumpGroupEpoch = false;
            for (Map.Entry<String, ResolvedRegularExpression> entry : resolvedRegularExpressions.entrySet()) {
                String regex = entry.getKey();

                // We can skip the regex if the group is no longer
                // subscribed to it.
                if (group.numSubscribedMembers(regex) == 0) continue;

                ResolvedRegularExpression newResolvedRegularExpression = entry.getValue();
                ResolvedRegularExpression oldResolvedRegularExpression = group
                    .resolvedRegularExpression(regex)
                    .orElse(ResolvedRegularExpression.EMPTY);

                if (!oldResolvedRegularExpression.topics.equals(newResolvedRegularExpression.topics)) {
                    bumpGroupEpoch = true;

                    oldResolvedRegularExpression.topics.forEach(topicName ->
                        subscribedTopicNames.compute(topicName, SubscriptionCount::decRegexCount)
                    );

                    newResolvedRegularExpression.topics.forEach(topicName ->
                        subscribedTopicNames.compute(topicName, SubscriptionCount::incRegexCount)
                    );
                }

                // Add the record to persist the change.
                records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    regex,
                    newResolvedRegularExpression
                ));
            }

            // Compute the subscription metadata.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNames,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        groupId, subscriptionMetadata);
                }
                bumpGroupEpoch = true;
                records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                int groupEpoch = group.groupEpoch() + 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
                metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
                group.setMetadataRefreshDeadline(
                    time.milliseconds() + METADATA_REFRESH_INTERVAL_MS,
                    groupEpoch
                );
            }
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Received result of regular expression resolution but " +
                "it no longer exists.", groupId);
        }

        return new CoordinatorResult<>(records);
    }

    /**
     * Check whether the member has updated its subscribed topic regular expression and
     * may trigger the resolution/the refresh of all the regular expressions in the
     * group. We align the refreshment of the regular expression in order to have
     * them trigger only one rebalance per update.
     *
     * @param context       The request context.
     * @param member        The old member.
     * @param updatedMember The new member.
     * @param records       The records accumulator.
     * @return Whether a rebalance must be triggered.
     */
    private boolean maybeUpdateRegularExpressions(
        AuthorizableRequestContext context,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String groupId = groupId();
        String memberId = updatedMember.memberId();
        String oldSubscribedTopicRegex = member.subscribedTopicRegex();
        String newSubscribedTopicRegex = updatedMember.subscribedTopicRegex();

        boolean bumpGroupEpoch = false;
        boolean requireRefresh = false;

        // Check whether the member has changed its subscribed regex.
        if (!Objects.equals(oldSubscribedTopicRegex, newSubscribedTopicRegex)) {
            log.debug("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                groupId, memberId, newSubscribedTopicRegex);

            if (isNotEmpty(oldSubscribedTopicRegex) && numSubscribedMembers(oldSubscribedTopicRegex) == 1) {
                // If the member was the last one subscribed to the regex, we delete the
                // resolved regular expression.
                records.add(newConsumerGroupRegularExpressionTombstone(
                    groupId,
                    oldSubscribedTopicRegex
                ));
            }

            if (isNotEmpty(newSubscribedTopicRegex)) {
                if (numSubscribedMembers(newSubscribedTopicRegex) == 0) {
                    // If the member subscribed to a new regex, we compile it to ensure its validity.
                    // We also trigger a refresh of the regexes in order to resolve it.
                    throwIfRegularExpressionIsInvalid(updatedMember.subscribedTopicRegex());
                    requireRefresh = true;
                } else {
                    // If the new regex is already resolved, we trigger a rebalance
                    // by bumping the group epoch.
                    bumpGroupEpoch = resolvedRegularExpression(newSubscribedTopicRegex).isPresent();
                }
            }
        }

        // Conditions to trigger a refresh:
        // 0. The group is subscribed to regular expressions.
        // 1. There is no ongoing refresh for the group.
        // 2. The last refresh is older than 10s.
        // 3. The group has unresolved regular expressions.
        // 4. The metadata image has new topics.

        // 0. The group is subscribed to regular expressions. We also take the one
        //    that the current may have just introduced.
        if (!requireRefresh && subscribedRegularExpressions().isEmpty()) {
            return bumpGroupEpoch;
        }

        // 1. There is no ongoing refresh for the group.
        String key = groupId() + "-regex";
        if (executor.isScheduled(key)) {
            return bumpGroupEpoch;
        }

        // 2. The last refresh is older than 10s. If the group does not have any regular
        //    expressions but the current member just brought a new one, we should continue.
        long lastRefreshTimeMs = lastResolvedRegularExpressionRefreshTimeMs();
        if (time.milliseconds() <= lastRefreshTimeMs + REGEX_BATCH_REFRESH_INTERVAL_MS) {
            return bumpGroupEpoch;
        }

        // 3. The group has unresolved regular expressions.
        Map<String, Integer> subscribedRegularExpressions = new HashMap<>(subscribedRegularExpressions());
        if (isNotEmpty(oldSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(oldSubscribedTopicRegex, Utils::decValue);
        }
        if (isNotEmpty(newSubscribedTopicRegex)) {
            subscribedRegularExpressions.compute(newSubscribedTopicRegex, Utils::incValue);
        }

        requireRefresh |= subscribedRegularExpressions.size() != numResolvedRegularExpressions();

        // 4. The metadata has new topics that we must consider.
        // TODO How to handle this?
        requireRefresh |= lastResolvedRegularExpressionVersion() < lastMetadataImageWithNewTopics;

        if (requireRefresh && !subscribedRegularExpressions.isEmpty()) {
            Set<String> regexes = Collections.unmodifiableSet(subscribedRegularExpressions.keySet());
            executor.schedule(
                key,
                () -> refreshRegularExpressions(context, groupId, log, time, metadataImage, authorizerPlugin, regexes),
                (result, exception) -> handleRegularExpressionsResult(groupId, memberId, result, exception)
            );
        }

        return bumpGroupEpoch;
    }

    /**
     * Updates the subscription metadata and bumps the group epoch if needed.
     *
     * @param bumpGroupEpoch    Whether the group epoch must be bumped.
     * @param member            The old member.
     * @param updatedMember     The new member.
     * @param records           The record accumulator.
     * @return The result of the update.
     */
    private GroupMetadataManager.UpdateSubscriptionMetadataResult updateSubscriptionMetadata(
        boolean bumpGroupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        final long currentTimeMs = time.milliseconds();
        final String groupId = groupId();
        int groupEpoch = groupEpoch();

        Map<String, Integer> subscribedRegularExpressions = computeSubscribedRegularExpressions(
            member,
            updatedMember
        );
        Map<String, SubscriptionCount> subscribedTopicNamesMap = computeSubscribedTopicNames(
            member,
            updatedMember
        );
        Map<String, TopicMetadata> subscriptionMetadata = computeSubscriptionMetadata(
            subscribedTopicNamesMap,
            metadataImage.topics(),
            metadataImage.cluster()
        );

        int numMembers = numMembers();
        if (!hasMember(updatedMember.memberId()) && !hasStaticMember(updatedMember.instanceId())) {
            numMembers++;
        }

        SubscriptionType subscriptionType = ConsumerGroup.subscriptionType(
            subscribedRegularExpressions,
            subscribedTopicNamesMap,
            numMembers
        );

        if (!subscriptionMetadata.equals(subscriptionMetadata())) {
            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                    groupId, subscriptionMetadata);
            }
            bumpGroupEpoch = true;
            records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
        }

        if (bumpGroupEpoch) {
            groupEpoch += 1;
            records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
            log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
            metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
        }

        setMetadataRefreshDeadline(currentTimeMs + METADATA_REFRESH_INTERVAL_MS, groupEpoch);

        return new GroupMetadataManager.UpdateSubscriptionMetadataResult(
            groupEpoch,
            subscriptionMetadata,
            subscriptionType
        );
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param groupEpoch            The group epoch.
     * @param member                The existing member.
     * @param updatedMember         The updated member.
     * @param subscriptionMetadata  The subscription metadata.
     * @param subscriptionType      The group subscription type.
     * @param records               The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        int groupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        Map<String, TopicMetadata> subscriptionMetadata,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records
    ) {
        String preferredServerAssignor = computePreferredServerAssignor(
            member,
            updatedMember
        ).orElse(defaultConsumerGroupAssignor.name()); // FIXME
        try {
            TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder assignmentResultBuilder =
                new TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder(groupId(), groupEpoch, consumerGroupAssignors.get(preferredServerAssignor))
                    .withMembers(members())
                    .withStaticMembers(staticMembers())
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(targetAssignment())
                    .withInvertedTargetAssignment(invertedTargetAssignment())
                    .withTopicsImage(metadataImage.topics())
                    .withResolvedRegularExpressions(resolvedRegularExpressions())
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            // If the instance id was associated to a different member, it means that the
            // static member is replaced by the current member hence we remove the previous one.
            String previousMemberId = staticMemberId(updatedMember.instanceId());
            if (previousMemberId != null && !updatedMember.memberId().equals(previousMemberId)) {
                assignmentResultBuilder.removeMember(previousMemberId);
            }

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                    groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs, assignmentResult.targetAssignment());
            } else {
                log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms.",
                    groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs);
            }

            records.addAll(assignmentResult.records());

            MemberAssignment newMemberAssignment = assignmentResult.targetAssignment().get(updatedMember.memberId());
            if (newMemberAssignment != null) {
                return new Assignment(newMemberAssignment.partitions());
            } else {
                return Assignment.EMPTY;
            }
        } catch (PartitionAssignorException ex) {
            String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                groupEpoch, ex.getMessage());
            log.error("[GroupId {}] {}.", groupId(), msg);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId               The group id.
     * @param member                The member to reconcile.
     * @param currentPartitionEpoch The function returning the current epoch of
     *                              a given partition.
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param ownedTopicPartitions  The list of partitions owned by the member. This
     *                              is reported in the ConsumerGroupHeartbeat API and
     *                              it could be null if not provided.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ConsumerGroupMember maybeReconcile(
        String groupId,
        ConsumerGroupMember member,
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .withCurrentPartitionEpoch(currentPartitionEpoch)
            .withOwnedTopicPartitions(ownedTopicPartitions)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupCurrentAssignmentRecord(groupId, updatedMember));

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Member {} new assignment state: epoch={}, previousEpoch={}, state={}, "
                        + "assignedPartitions={} and revokedPartitions={}.",
                    groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                    assignmentToString(updatedMember.assignedPartitions()), assignmentToString(updatedMember.partitionsPendingRevocation()));
            }

            // Schedule/cancel the rebalance timeout if the member uses the consumer protocol.
            // The members using classic protocol only have join timer and sync timer.
            if (!updatedMember.useClassicProtocol()) {
                if (updatedMember.state() == MemberState.UNREVOKED_PARTITIONS) {
                    scheduleConsumerGroupRebalanceTimeout(
                        groupId,
                        updatedMember.memberId(),
                        updatedMember.memberEpoch(),
                        updatedMember.rebalanceTimeoutMs()
                    );
                } else {
                    cancelGroupRebalanceTimeout(groupId, updatedMember.memberId());
                }
            }
        }

        return updatedMember;
    }

    private void cancelGroupRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(groupRebalanceTimeoutKey(groupId, memberId));
    }

    public static String groupRebalanceTimeoutKey(String groupId, String memberId) {
        return "rebalance-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Schedules a rebalance timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupRebalanceTimeout(
        String groupId,
        String memberId,
        int memberEpoch,
        int rebalanceTimeoutMs
    ) {
        String key = groupRebalanceTimeoutKey(groupId, memberId);
        timer.schedule(key, rebalanceTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = consumerGroup(groupId);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

                if (member.memberEpoch() == memberEpoch) {
                    log.info("[GroupId {}] Member {} fenced from the group because " +
                            "it failed to transition from epoch {} within {}ms.",
                        groupId, memberId, memberEpoch, rebalanceTimeoutMs);

                    return consumerGroupFenceMember(group, member, null);
                } else {
                    log.debug("[GroupId {}] Ignoring rebalance timeout for {} because the member " +
                        "left the epoch {}.", groupId, memberId, memberEpoch);
                    return new CoordinatorResult<>(List.of());
                }
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(List.of());
        });
    }

    /**
     * Fences a member from a consumer group and maybe downgrade the consumer group to a classic group.
     *
     * @param group     The group.
     * @param member    The member.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMember(
        ConsumerGroup group,
        ConsumerGroupMember member,
        T response
    ) {
        return consumerGroupFenceMembers(group, Set.of(member), response);
    }

    /**
     * Fences members from a consumer group and maybe downgrade the consumer group to a classic group.
     *
     * @param group     The group.
     * @param members   The members.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMembers(
        ConsumerGroup group,
        Set<ConsumerGroupMember> members,
        T response
    ) {
        if (members.isEmpty()) {
            // No members to fence. Don't bump the group epoch.
            return new CoordinatorResult<>(List.of(), response);
        }

        List<CoordinatorRecord> records = new ArrayList<>();
        if (validateOnlineDowngradeWithFencedMembers(group, members)) {
            convertToClassicGroup(group, members, null, records);
            return new CoordinatorResult<>(records, response, null, false);
        } else {
            for (ConsumerGroupMember member : members) {
                removeMember(records, group.groupId(), member.memberId());
            }

            // Check whether resolved regular expressions could be deleted.
            Set<String> deletedRegexes = maybeDeleteResolvedRegularExpressions(
                records,
                group,
                members
            );

            // We update the subscription metadata without the leaving members.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                group.computeSubscribedTopicNamesWithoutDeletedMembers(members, deletedRegexes),
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        group.groupId(), subscriptionMetadata);
                }
                records.add(newConsumerGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
            }

            // We bump the group epoch.
            int groupEpoch = group.groupEpoch() + 1;
            records.add(newConsumerGroupEpochRecord(group.groupId(), groupEpoch));
            log.info("[GroupId {}] Bumped group epoch to {}.", group.groupId(), groupEpoch);

            for (ConsumerGroupMember member : members) {
                cancelTimers(group.groupId(), member.memberId());
            }

            return new CoordinatorResult<>(records, response);
        }
    }

    /**
     * Validates the online downgrade if consumer members are fenced from the consumer group.
     *
     * @param consumerGroup     The ConsumerGroup.
     * @param fencedMembers     The fenced members.
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithFencedMembers(ConsumerGroup consumerGroup, Set<ConsumerGroupMember> fencedMembers) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(fencedMembers)) {
            return false;
        } else if (consumerGroup.numMembers() - fencedMembers.size() <= 0) {
            log.debug("Skip downgrading the consumer group {} to classic group because it's empty.",
                consumerGroup.groupId());
            return false;
        } else if (!config.consumerGroupMigrationPolicy().isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() - fencedMembers.size() > config.classicGroupMaxSize()) {
            log.info("Cannot downgrade consumer group {} to classic group because its group size is greater than classic group max size.",
                consumerGroup.groupId());
            return false;
        }
        return true;
    }

    /**
     * Handles a regular heartbeat from a consumer group member. It mainly consists of
     * three parts:
     * 1) The member is created or updated. The group epoch is bumped if the member
     *    has been created or updated.
     * 2) The target assignment for the consumer group is updated if the group epoch
     *    is larger than the current target assignment epoch.
     * 3) The member's assignment is reconciled with the target assignment.
     *
     * @param context               The request context.
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param subscribedTopicNames  The list of subscribed topic names from the request
     *                              or null.
     * @param subscribedTopicRegex  The regular expression based subscription from the request
     *                              or null.
     * @param assignorName          The assignor name from the request or null.
     * @param ownedTopicPartitions  The list of owned partitions from the request or null.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        AuthorizableRequestContext context,
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String assignorName,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) throws ApiException {
        throwIfConsumerGroupIsFull(memberId);

        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final boolean createIfNotExists = memberEpoch == 0;

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        final ConsumerGroupMember member;
        if (instanceId == null) {
            member = getOrMaybeSubscribeDynamicConsumerGroupMember(
                memberId,
                memberEpoch,
                ownedTopicPartitions,
                createIfNotExists,
                false
            );
        } else {
            member = getOrMaybeSubscribeStaticConsumerGroupMember(
                memberId,
                memberEpoch,
                instanceId,
                ownedTopicPartitions,
                createIfNotExists,
                false,
                records
            );
        }

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateServerAssignorName(Optional.ofNullable(assignorName))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .maybeUpdateSubscribedTopicRegex(Optional.ofNullable(subscribedTopicRegex))
            .setClientId(context.clientId())
            .setClientHost(context.clientAddress().toString())
            .setClassicMemberMetadata(null)
            .build();

        // If the group is newly created, we must ensure that it moves away from
        // epoch 0 and that it is fully initialized.
        boolean bumpGroupEpoch = groupEpoch() == 0;

        bumpGroupEpoch |= hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        );

        bumpGroupEpoch |= maybeUpdateRegularExpressions(
            context,
            member,
            updatedMember,
            records
        );

        int groupEpoch = groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = subscriptionMetadata();
        SubscriptionType subscriptionType = subscriptionType();

        if (bumpGroupEpoch || hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            GroupMetadataManager.UpdateSubscriptionMetadataResult result = updateSubscriptionMetadata(
                bumpGroupEpoch,
                member,
                updatedMember,
                records
            );

            groupEpoch = result.groupEpoch;
            subscriptionMetadata = result.subscriptionMetadata;
            subscriptionType = result.subscriptionType;
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The delta between
        // the existing and the new target assignment is persisted to the partition.
        final int targetAssignmentEpoch;
        final Assignment targetAssignment;

        if (groupEpoch > assignmentEpoch()) {
            targetAssignment = updateTargetAssignment(
                groupEpoch,
                member,
                updatedMember,
                subscriptionMetadata,
                subscriptionType,
                records
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = assignmentEpoch();
            targetAssignment = targetAssignment(updatedMember.memberId(), updatedMember.instanceId());
        }

        // 3. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = maybeReconcile(
            groupId,
            updatedMember,
            this::currentPartitionEpoch,
            targetAssignmentEpoch,
            targetAssignment,
            ownedTopicPartitions,
            records
        );

        scheduleConsumerGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member sent a full request. It does so when joining or rejoining the group with zero
        //    as the member epoch; or on any errors (e.g. timeout). We use all the non-optional fields
        //    (rebalanceTimeoutMs, (subscribedTopicNames or subscribedTopicRegex) and ownedTopicPartitions)
        //    to detect a full request as those must be set in a full request.
        // 2. The member's assignment has been updated.
        boolean isFullRequest = rebalanceTimeoutMs != -1 && (subscribedTopicNames != null || subscribedTopicRegex != null) && ownedTopicPartitions != null;
        if (memberEpoch == 0 || isFullRequest || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(ConsumerGroupHeartbeatResponse.createAssignment(updatedMember.assignedPartitions()));
        }

        return new CoordinatorResult<>(records, response);
    }

}
