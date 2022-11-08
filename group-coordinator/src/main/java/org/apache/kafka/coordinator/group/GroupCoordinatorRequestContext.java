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

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.utils.BufferSupplier;

import java.net.InetAddress;

public class GroupCoordinatorRequestContext {
    private final RequestHeaderData requestHeader;

    private final InetAddress clientAddress;

    private final BufferSupplier bufferSupplier;

    public GroupCoordinatorRequestContext(
        RequestHeaderData requestHeader,
        InetAddress clientAddress,
        BufferSupplier bufferSupplier
    ) {
        this.requestHeader = requestHeader;
        this.clientAddress = clientAddress;
        this.bufferSupplier = bufferSupplier;
    }

    public RequestHeaderData requestHeader() {
        return this.requestHeader;
    }

    public InetAddress clientAddress() {
        return this.clientAddress;
    }

    public BufferSupplier bufferSupplier() {
        return this.bufferSupplier;
    }
}
