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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

/**
 * A set of factory methods that make {@link Protos.Event Event}s easier to create.
 */
public final class SchedulerEvents {

    private SchedulerEvents() {}

    /**
     * Utility method to more succinctly construct an {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#SUBSCRIBED SUBSCRIBED}.
     *
     * @param frameworkId              The frameworkId to be set on the
     *                                 {@link org.apache.mesos.v1.scheduler.Protos.Event.Subscribed} message.
     * @param heartbeatIntervalSeconds The heartbeatIntervalSeconds to be set on the
     *                                 {@link org.apache.mesos.v1.scheduler.Protos.Event.Subscribed} message.
     * @return An instance of {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#SUBSCRIBED SUBSCRIBED} and
     * {@link Protos.Event#getSubscribed() subscribed} set based on the provide parameters.
     */
    @NotNull
    public static Protos.Event subscribed(@NotNull final String frameworkId, final int heartbeatIntervalSeconds) {
        return Protos.Event.newBuilder()
            .setType(Protos.Event.Type.SUBSCRIBED)
            .setSubscribed(
                Protos.Event.Subscribed.newBuilder()
                    .setFrameworkId(org.apache.mesos.v1.Protos.FrameworkID.newBuilder()
                        .setValue(frameworkId)
                    )
                    .setHeartbeatIntervalSeconds(heartbeatIntervalSeconds)
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct an {@link Protos.Event Event} of type
     * {@link Protos.Event.Type#OFFERS OFFERS}.
     *
     * @param hostname    The hostname to set on the offer.
     * @param offerId     The offerId to set on the offer.
     * @param agentId     The agentId to set on the offer.
     * @param frameworkId The frameworkId to set on the offer.
     * @param cpus        The number of cpus the offer will have.
     * @param mem         The number of megabytes of memory the offer will have.
     * @param disk        The number of megabytes of disk the offer will have.
     * @return An {@link Protos.Event Event} of type
     * {@link Protos.Event.Type#OFFERS OFFERS} containing a single
     * {@link org.apache.mesos.v1.Protos.Offer Offer} using the specified parameters as the values of the offer.
     */
    @NotNull
    public static Protos.Event resourceOffer(
        @NotNull final String hostname,
        @NotNull final String offerId,
        @NotNull final String agentId,
        @NotNull final String frameworkId,
        final double cpus,
        final long mem,
        final long disk
    ) {
        return Protos.Event.newBuilder()
            .setType(Protos.Event.Type.OFFERS)
            .setOffers(
                Protos.Event.Offers.newBuilder()
                    .addAllOffers(Lists.newArrayList(
                        org.apache.mesos.v1.Protos.Offer.newBuilder()
                            .setHostname(hostname)
                            .setId(org.apache.mesos.v1.Protos.OfferID.newBuilder().setValue(offerId))
                            .setAgentId(org.apache.mesos.v1.Protos.AgentID.newBuilder().setValue(agentId))
                            .setFrameworkId(org.apache.mesos.v1.Protos.FrameworkID.newBuilder().setValue(frameworkId))
                            .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                                .setName("cpus")
                                .setRole("*")
                                .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                                .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue(cpus)))
                            .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                                .setName("mem")
                                .setRole("*")
                                .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                                .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue(mem)))
                            .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                                .setName("disk")
                                .setRole("*")
                                .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                                .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue(disk)))
                            .build()
                    ))
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct an {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#MESSAGE MESSAGE}.
     *
     * @param agentId        The {@link org.apache.mesos.v1.Protos.AgentID#getValue() value} of the
     *                       {@link Protos.Event.Message#getAgentId() agentId} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @param executorId     The {@link org.apache.mesos.v1.Protos.ExecutorID#getValue() value} of the
     *                       {@link Protos.Event.Message#getExecutorId() executorId} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @param data           The {@link Protos.Event.Message#getData() data} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @return  A {@link Protos.Call Call} with a configured {@link Protos.Call.Acknowledge Acknowledge}.
     */
    @NotNull
    public static Protos.Event message(
        @NotNull final String agentId,
        @NotNull final String executorId,
        @NotNull final ByteString data
    ) {
        return message(
            org.apache.mesos.v1.Protos.AgentID.newBuilder().setValue(agentId).build(),
            org.apache.mesos.v1.Protos.ExecutorID.newBuilder().setValue(executorId).build(),
            data
        );
    }

    /**
     * Utility method to more succinctly construct an {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#MESSAGE MESSAGE}.
     *
     * @param agentId        The {@link Protos.Event.Message#getAgentId() agentId} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @param executorId     The {@link Protos.Event.Message#getExecutorId() executorId} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @param data           The {@link Protos.Event.Message#getData() data} to be set on the
     *                       {@link Protos.Event.Message Message}.
     * @return  A {@link Protos.Call Call} with a configured {@link Protos.Call.Acknowledge Acknowledge}.
     */
    @NotNull
    public static Protos.Event message(
        @NotNull final org.apache.mesos.v1.Protos.AgentID agentId,
        @NotNull final org.apache.mesos.v1.Protos.ExecutorID executorId,
        @NotNull final ByteString data
    ) {
        return Protos.Event.newBuilder()
            .setType(Protos.Event.Type.MESSAGE)
            .setMessage(
                Protos.Event.Message.newBuilder()
                    .setAgentId(agentId)
                    .setExecutorId(executorId)
                    .setData(data)
            )
            .build();
    }

}
