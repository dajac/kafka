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

import java.util.List;
import java.util.Objects;

class Result<T> {
    private final List<Record> records;
    private final T response;
    private final boolean isAtomic;

    public Result(
        List<Record> records,
        T response,
        boolean isAtomic
    ) {
        Objects.requireNonNull(records);
        Objects.requireNonNull(response);

        this.records = records;
        this.response = response;
        this.isAtomic = isAtomic;
    }

    public List<Record> records() {
        return records;
    }

    public T response() {
        return response;
    }

    public boolean isAtomic() {
        return isAtomic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Result<?> result = (Result<?>) o;

        if (isAtomic != result.isAtomic) return false;
        if (!records.equals(result.records)) return false;
        return response.equals(result.response);
    }

    @Override
    public int hashCode() {
        int result = records.hashCode();
        result = 31 * result + response.hashCode();
        result = 31 * result + (isAtomic ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Result(records=" + records +
            ", response=" + response +
            ", isAtomic=" + isAtomic +
            ")";
    }
}
