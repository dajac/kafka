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

import java.util.Arrays;
import java.util.Objects;

public class TopicMetadata {

    private final Uuid id;

    private final String name;

    private final int[] partitions;

    public TopicMetadata(
        Uuid id,
        String name,
        int[] partitions
    ) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(name);
        Objects.requireNonNull(partitions);

        this.id = id;
        this.name = name;
        this.partitions = partitions;
    }

    public Uuid id() {
        return this.id;
    }

    public String name() {
        return this.name;
    }

    public int[] partitions() {
        return this.partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicMetadata that = (TopicMetadata) o;

        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        return Arrays.equals(
            partitions,
            that.partitions
        );
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + Arrays.hashCode(partitions);
        return result;
    }

    @Override
    public String toString() {
        return "TopicMetadata(" +
            "id=" + id +
            ", name=" + name +
            ", partitions=" + Arrays.toString(partitions) +
            ')';
    }
}
