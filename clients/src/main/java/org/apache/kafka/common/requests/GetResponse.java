/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.GET.id);
    private static final String VALUE_NAME = "value";
    private final byte[] _val;
    private final int _error;

    public GetResponse(byte[] val, int error) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.GET.id, 0)));
        struct.set(VALUE_NAME, val);
        _val = val;
        _error = error;
    }

    public GetResponse(Struct struct) {
        super(struct);
        _val = (byte[]) struct.get(VALUE_NAME);
        _error = (int) struct.get("error");
    }

    public byte[] getVal() {
        return this._val;
    }
    public int getError() {
        return this._error;
    }

    public static GetResponse parse(ByteBuffer buffer) {
        return new GetResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }

    public static GetResponse parse(ByteBuffer buffer, int version) {
        return new GetResponse((Struct) ProtoUtils.responseSchema(ApiKeys.GET.id, version).read(buffer));
    }
}
