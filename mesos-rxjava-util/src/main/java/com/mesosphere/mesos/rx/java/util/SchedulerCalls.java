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

package com.mesosphere.mesos.rx.java.util;

import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static org.apache.mesos.v1.scheduler.Protos.Call.*;

/**
 * A set of factory methods that make {@link Call Call}s easier to create.
 */
public final class SchedulerCalls {

    private SchedulerCalls() {}

    /**
     * Utility method to more succinctly construct a {@link Call Call} of type {@link Type#ACKNOWLEDGE ACKNOWLEDGE}.
     * <p>
     *
     * @param frameworkId    The {@link Protos.FrameworkID} to be set on the {@link Call}
     * @param uuid           The {@link Protos.TaskStatus#getUuid() uuid} from the
     *                       {@link org.apache.mesos.v1.scheduler.Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @param agentId        The {@link Protos.TaskStatus#getAgentId() agentId} from the
     *                       {@link org.apache.mesos.v1.scheduler.Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @param taskId         The {@link Protos.TaskStatus#getTaskId() taskId} from the
     *                       {@link org.apache.mesos.v1.scheduler.Protos.Event.Update#getStatus() TaskStatus} received from Mesos.
     * @return  A {@link Call} with a configured {@link Acknowledge}.
     */
    @NotNull
    public static Call ackUpdate(
        @NotNull final Protos.FrameworkID frameworkId,
        @NotNull final ByteString uuid,
        @NotNull final Protos.AgentID agentId,
        @NotNull final Protos.TaskID taskId
    ) {
        return newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Type.ACKNOWLEDGE)
            .setAcknowledge(
                Acknowledge.newBuilder()
                    .setUuid(uuid)
                    .setAgentId(agentId)
                    .setTaskId(taskId)
                    .build()
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct a {@link Call Call} of type {@link Type#DECLINE DECLINE}.
     * <p>
     *
     * @param frameworkId    The {@link Protos.FrameworkID FrameworkID} to be set on the {@link Call}
     * @param offerIds       A list of {@link Protos.OfferID OfferID} from the
     *                       {@link Protos.Offer}s received from Mesos.
     * @return  A {@link Call} with a configured {@link Decline}.
     */
    @NotNull
    public static Call decline(@NotNull final Protos.FrameworkID frameworkId, @NotNull final List<Protos.OfferID> offerIds) {
        return newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Type.DECLINE)
            .setDecline(
                Decline.newBuilder()
                    .addAllOfferIds(offerIds)
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct a {@link Call Call} of type {@link Type#SUBSCRIBE SUBSCRIBE}.
     * <p>
     *
     * @param frameworkId               The frameworkId to set on the {@link Protos.FrameworkInfo FrameworkInfo} and
     *                                  {@link Call Call} messages.
     * @param user                      The user to set on the {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @param frameworkName             The name to set on the {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @param failoverTimeoutSeconds    The failoverTimeoutSeconds to set on the
     *                                  {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @return An {@link Call Call} of type {@link Type#SUBSCRIBE SUBSCRIBE} with the configured
     * {@link Subscribe Subscribe} sub-message.
     */
    @NotNull
    public static Call subscribe(
        @NotNull final String frameworkId,
        @NotNull final String user,
        @NotNull final String frameworkName,
        final long failoverTimeoutSeconds
    ) {
        final Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue(frameworkId).build();
        return subscribe(frameworkID, user, frameworkName, failoverTimeoutSeconds);
    }

    /**
     * Utility method to more succinctly construct a {@link Call Call} of type {@link Type#SUBSCRIBE SUBSCRIBE}.
     * <p>
     *
     * @param frameworkId               The frameworkId to set on the {@link Protos.FrameworkInfo FrameworkInfo} and
     *                                  {@link Call Call} messages.
     * @param user                      The user to set on the {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @param frameworkName             The name to set on the {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @param failoverTimeoutSeconds    The failoverTimeoutSeconds to set on the
     *                                  {@link Protos.FrameworkInfo FrameworkInfo} message.
     * @return An {@link Call Call} of type {@link Type#SUBSCRIBE SUBSCRIBE} with the configured
     * {@link Subscribe Subscribe} sub-message.
     */
    @NotNull
    public static Call subscribe(
        @NotNull final Protos.FrameworkID frameworkId,
        @NotNull final String user,
        @NotNull final String frameworkName,
        final long failoverTimeoutSeconds
    ) {
        return newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Type.SUBSCRIBE)
            .setSubscribe(
                Subscribe.newBuilder()
                    .setFrameworkInfo(
                        Protos.FrameworkInfo.newBuilder()
                            .setId(frameworkId)
                            .setUser(user)
                            .setName(frameworkName)
                            .setFailoverTimeout(failoverTimeoutSeconds)
                            .build()
                    )
            )
            .build();
    }
}
