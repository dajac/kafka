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

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Objects;

class Record {
    private final ApiMessageAndVersion key;
    private final ApiMessageAndVersion value;

    public Record(
        ApiMessageAndVersion key,
        ApiMessageAndVersion value
    ) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        this.key = key;
        this.value = value;
    }

    public ApiMessageAndVersion key() {
        return this.key;
    }

    public ApiMessageAndVersion value() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        if (!key.equals(record.key)) return false;
        return value.equals(record.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Record(key=" + key + ", value=" + value + ")";
    }
}
