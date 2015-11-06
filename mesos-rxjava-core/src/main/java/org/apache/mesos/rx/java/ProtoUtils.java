/*
 *    Copyright (C) 2015 Apache Software Foundation (ASF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mesos.rx.java;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.regex.Pattern;

public final class ProtoUtils {
    private static final Pattern PROTO_TO_STRING = Pattern.compile(" *\\n *");

    private ProtoUtils() {}

    @NotNull
    public static Protos.Call ackUpdate(
        @NotNull FrameworkID frameworkId,
        @NotNull final ByteString uuid,
        @NotNull final AgentID agentId,
        @NotNull final TaskID taskId
    ) {
        return Protos.Call.newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Protos.Call.Type.ACKNOWLEDGE)
            .setAcknowledge(
                Protos.Call.Acknowledge.newBuilder()
                    .setUuid(uuid)
                    .setAgentId(agentId)
                    .setTaskId(taskId)
                    .build()
            )
            .build();
    }

    @NotNull
    public static String protoToString(@NotNull final Message message) {
        return PROTO_TO_STRING.matcher(message.toString()).replaceAll(" ").trim();
    }

    @NotNull
    public static Protos.Call decline(@NotNull final FrameworkID frameworkId, @NotNull final List<OfferID> offerIds) {
        return Protos.Call.newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Protos.Call.Type.DECLINE)
            .setDecline(
                Protos.Call.Decline.newBuilder()
                    .addAllOfferIds(offerIds)
            )
            .build();
    }
}
