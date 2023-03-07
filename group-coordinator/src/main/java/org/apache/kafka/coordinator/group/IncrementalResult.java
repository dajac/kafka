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

public class IncrementalResult<T> {

    interface IncrementalResultOp<T> {
        Result<T> generateRecordsAndResponse(T request) throws Exception;
    }

    final private T initialRequest;
    final private List<IncrementalResultOp<T>> steps;

    public IncrementalResult(
        T initialRequest,
        List<IncrementalResultOp<T>> steps
    ) {
        Objects.requireNonNull(initialRequest);
        Objects.requireNonNull(steps);

        this.initialRequest = initialRequest;
        this.steps = Collections.unmodifiableList(steps);
    }

    public T initialRequest() {
        return initialRequest;
    }

    public List<IncrementalResultOp<T>> steps() {
        return steps;
    }
}
