/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

public class GetRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.GET.id);
    private static final String KEY = "key";
    private final String _reqKey;

    public GetRequest(String reqKey) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(KEY, reqKey);
        _reqKey = reqKey;
    }

    public GetRequest(Struct struct) {
        super(struct);
        _reqKey = struct.getString(KEY);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
          return new GetResponse(new byte[] {}, 1);
    }

    public static GetRequest parse(ByteBuffer buffer, int versionId) {
        return new GetRequest(ProtoUtils.parseRequest(ApiKeys.GET.id, versionId, buffer));
    }

    public static GetRequest parse(ByteBuffer buffer) {
        return new GetRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
