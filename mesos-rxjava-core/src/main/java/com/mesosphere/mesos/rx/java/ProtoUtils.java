/*
 *    Copyright (C) 2015 Mesosphere, Inc
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

package com.mesosphere.mesos.rx.java;

import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.regex.Pattern;

/**
 * A set of utility methods that make working with Mesos Protos easier.
 */
public final class ProtoUtils {
    private static final Pattern PROTO_TO_STRING = Pattern.compile(" *\\n *");

    private ProtoUtils() {}

    /**
     * Convenience method to create an {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#ACKNOWLEDGE}
     * {@link org.apache.mesos.v1.scheduler.Protos.Call}.
     * @param frameworkId    The {@link FrameworkID} to be set on the {@link org.apache.mesos.v1.scheduler.Protos.Call}
     * @param uuid           The {@link org.apache.mesos.v1.Protos.TaskStatus#getUuid() uuid} from the
     *                       {@link Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @param agentId        The {@link org.apache.mesos.v1.Protos.TaskStatus#getAgentId() agentId} from the
     *                       {@link Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @param taskId         The {@link org.apache.mesos.v1.Protos.TaskStatus#getTaskId() taskId} from the
     *                       {@link Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @return  A {@link org.apache.mesos.v1.scheduler.Protos.Call} with a configured
     *          {@link org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge}.
     */
    @NotNull
    public static Protos.Call ackUpdate(
        @NotNull final FrameworkID frameworkId,
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

    /**
     * Utility method to make logging of protos a little better than what is done by default.
     * @param message    The proto message to be "stringified"
     * @return  A string representation of the provided {@code message} surrounded with curly braces ('{}') and all
     *          new lines replaced with a space.
     */
    @NotNull
    public static String protoToString(@NotNull final Object message) {
        return "{ " + PROTO_TO_STRING.matcher(message.toString()).replaceAll(" ").trim() + " }";
    }

    /**
     * Convenience method to create an {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#DECLINE}
     * {@link org.apache.mesos.v1.scheduler.Protos.Call}.
     * @param frameworkId    The {@link FrameworkID} to be set on the {@link org.apache.mesos.v1.scheduler.Protos.Call}
     * @param offerIds       A list of {@link org.apache.mesos.v1.Protos.OfferID} from the
     *                       {@link org.apache.mesos.v1.Protos.Offer}s received from Mesos.
     * @return  A {@link org.apache.mesos.v1.scheduler.Protos.Call} with a configured
     *          {@link org.apache.mesos.v1.scheduler.Protos.Call.Decline}.
     */
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
