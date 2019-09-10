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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

public class RequestContext {
    public final RequestHeader header;
    public final String connectionId;
    public final InetAddress clientAddress;
    public final KafkaPrincipal principal;
    public final ListenerName listenerName;
    public final SecurityProtocol securityProtocol;
    public final String clientName;
    public final String clientVersion;

    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol) {
        this.header = header;
        this.connectionId = connectionId;
        this.clientAddress = clientAddress;
        this.principal = principal;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.clientName = "";
        this.clientVersion = "";
    }

    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol,
                          String clientName,
                          String clientVersion) {
        this.header = header;
        this.connectionId = connectionId;
        this.clientAddress = clientAddress;
        this.principal = principal;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.clientName = clientName;
        this.clientVersion = clientVersion;
    }

    public RequestAndSize parseRequest(ByteBuffer buffer) {
        ApiKeys apiKey = header.apiKey();
        try {
            if (isUnsupportedApiVersionsRequest()) {
                short apiVersion = API_VERSIONS.latestVersion();
                Struct struct = apiKey.parseRequest(apiVersion, buffer);
                ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(struct, apiVersion, header.apiVersion());
                return new RequestAndSize(apiVersionsRequest, struct.sizeOf());
            } else {
                short apiVersion = header.apiVersion();
                Struct struct = apiKey.parseRequest(apiVersion, buffer);
                AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
                return new RequestAndSize(body, struct.sizeOf());
            }
        } catch (Throwable ex) {
            throw new InvalidRequestException("Error getting request for apiKey: " + apiKey +
                ", apiVersion: " + header.apiVersion() +
                ", connectionId: " + connectionId +
                ", listenerName: " + listenerName +
                ", principal: " + principal, ex);
        }
    }

    public Send buildResponse(AbstractResponse body) {
        ResponseHeader responseHeader = header.toResponseHeader();
        return body.toSend(connectionId, responseHeader, apiVersion());
    }

    private boolean isUnsupportedApiVersionsRequest() {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    public short apiVersion() {
        // Use latest when serializing an unhandled ApiVersion response
        if (isUnsupportedApiVersionsRequest())
            return API_VERSIONS.latestVersion();
        return header.apiVersion();
    }

}
