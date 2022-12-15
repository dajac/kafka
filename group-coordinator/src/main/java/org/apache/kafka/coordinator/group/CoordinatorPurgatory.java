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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

public class CoordinatorPurgatory {
    private final TreeMap<Long, List<DeferredEvent>> pending = new TreeMap<>();

    void completeUpTo(long offset) {
        Iterator<Map.Entry<Long, List<DeferredEvent>>> iter = pending.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, List<DeferredEvent>> entry = iter.next();
            if (entry.getKey() > offset) {
                break;
            }
            for (DeferredEvent event : entry.getValue()) {
                event.complete();
            }
            iter.remove();
        }
    }

    void failAll(Exception exception) {
        Iterator<Map.Entry<Long, List<DeferredEvent>>> iter = pending.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, List<DeferredEvent>> entry = iter.next();
            for (DeferredEvent event : entry.getValue()) {
                event.completeExceptionally(exception);
            }
            iter.remove();
        }
    }

    void add(long offset, DeferredEvent event) {
        if (!pending.isEmpty()) {
            long lastKey = pending.lastKey();
            if (offset < lastKey) {
                throw new RuntimeException("There is already a purgatory event with " +
                    "offset " + lastKey + ".  We should not add one with an offset of " +
                    offset + " which " + "is lower than that.");
            }
        }
        List<DeferredEvent> events = pending.get(offset);
        if (events == null) {
            events = new ArrayList<>();
            pending.put(offset, events);
        }
        events.add(event);
    }

    OptionalLong highestPendingOffset() {
        if (pending.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(pending.lastKey());
        }
    }
}
