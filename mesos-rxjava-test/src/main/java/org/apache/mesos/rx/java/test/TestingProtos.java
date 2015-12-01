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

package org.apache.mesos.rx.java.test;

import com.google.common.collect.Lists;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

import static org.apache.mesos.v1.Protos.*;

/**
 * A set of utilities that are useful when testing code that requires instances of Mesos Protos
 */
public final class TestingProtos {

    private TestingProtos() {}

    /**
     * A predefined instance of {@link org.apache.mesos.v1.scheduler.Protos.Event Event} representing a
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#HEARTBEAT HEARTBEAT} message.
     */
    @NotNull
    public static final Protos.Event HEARTBEAT = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.HEARTBEAT)
        .build();

    /**
     * A predefined instance of {@link org.apache.mesos.v1.scheduler.Protos.Event Event} representing a
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#SUBSCRIBED SUBSCRIBED} message with the following values
     * set:
     * <ul>
     * <li>{@code frameworkId = "a7cfd25c-79bd-481c-91cc-692e5db1ec3d"}</li>
     * <li>{@code hearbeatIntervalSeconds = 15}</li>
     * </ul>
     */
    @NotNull
    public static final Protos.Event SUBSCRIBED = subscribed("a7cfd25c-79bd-481c-91cc-692e5db1ec3d", 15);

    /**
     * A predefined instance of {@link org.apache.mesos.v1.scheduler.Protos.Event Event} representing a
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#OFFERS OFFERS} message containing a single offer with the
     * following values set:
     * <ul>
     * <li>{@code hostname = "host1"}</li>
     * <li>{@code offerId = "offer1"}</li>
     * <li>{@code agentId = "agent1"}</li>
     * <li>{@code frameworkId = "frw1"}</li>
     * <li>{@code cpus = 8.0}</li>
     * <li>{@code mem = 8192}</li>
     * <li>{@code disk = 8192}</li>
     * </ul>
     */
    @NotNull
    public static final Protos.Event OFFER = resourceOffer("host1", "offer1", "agent1", "frw1", 8d, 8192, 8192);

    /**
     * A predefined instance of {@link org.apache.mesos.v1.scheduler.Protos.Call Call} representing a
     * {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#SUBSCRIBE SUBSCRIBE} message with the following values set:
     * <ul>
     * <li>{@code frameworkId = "a7cfd25c-79bd-481c-91cc-692e5db1ec3d"}</li>
     * <li>{@code user = "unit-test-user"}</li>
     * <li>{@code frameworkName = "unit-testing"}</li>
     * </ul>
     */
    @NotNull
    public static final Protos.Call SUBSCRIBE = subscribe(
        "a7cfd25c-79bd-481c-91cc-692e5db1ec3d", "unit-test-user", "unit-testing"
    );

    /**
     * Utility method to more succinctly construct an {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#SUBSCRIBED}.
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
                    .setFrameworkId(FrameworkID.newBuilder()
                        .setValue(frameworkId)
                    )
                    .setHeartbeatIntervalSeconds(heartbeatIntervalSeconds)
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct an {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#OFFERS}.
     *
     * @param hostname    The hostname to set on the offer.
     * @param offerId     The offerId to set on the offer.
     * @param agentId     The agentId to set on the offer.
     * @param frameworkId The frameworkId to set on the offer.
     * @param cpus        The number of cpus the offer will have.
     * @param mem         The number of megabytes of memory the offer will have.
     * @param disk        The number of megabytes of disk the offer will have.
     * @return An {@link org.apache.mesos.v1.scheduler.Protos.Event Event} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Event.Type#OFFERS OFFERS} containing a single
     * {@link Offer Offer} using the specified parameters as the values of the offer.
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
                        Offer.newBuilder()
                            .setHostname(hostname)
                            .setId(OfferID.newBuilder().setValue(offerId))
                            .setAgentId(AgentID.newBuilder().setValue(agentId))
                            .setFrameworkId(FrameworkID.newBuilder().setValue(frameworkId))
                            .addResources(Resource.newBuilder()
                                .setName("cpus")
                                .setRole("*")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(cpus)))
                            .addResources(Resource.newBuilder()
                                .setName("mem")
                                .setRole("*")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(mem)))
                            .addResources(Resource.newBuilder()
                                .setName("disk")
                                .setRole("*")
                                .setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(disk)))
                            .build()
                    ))
            )
            .build();
    }

    /**
     * Utility method to more succinctly construct a {@link org.apache.mesos.v1.scheduler.Protos.Call Call} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#SUBSCRIBE}.
     *
     * @param frameworkId   The frameworkId to set on the {@link FrameworkInfo} and
     *                      {@link org.apache.mesos.v1.scheduler.Protos.Call Call} messages.
     * @param user          The user to set on the {@link FrameworkInfo} message.
     * @param frameworkName The name to set on the {@link FrameworkInfo} message.
     * @return An {@link org.apache.mesos.v1.scheduler.Protos.Call Call} of type
     * {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#SUBSCRIBE SUBSCRIBE} with the configured
     * {@link org.apache.mesos.v1.scheduler.Protos.Call.Subscribe Subscribe} sub-message.
     */
    @NotNull
    public static Protos.Call subscribe(
        @NotNull final String frameworkId,
        @NotNull final String user,
        @NotNull final String frameworkName
    ) {
        final FrameworkID frameworkID = FrameworkID.newBuilder().setValue(frameworkId).build();
        return Protos.Call.newBuilder()
            .setFrameworkId(frameworkID)
            .setType(Protos.Call.Type.SUBSCRIBE)
            .setSubscribe(
                Protos.Call.Subscribe.newBuilder()
                    .setFrameworkInfo(
                        FrameworkInfo.newBuilder()
                            .setId(frameworkID)
                            .setUser(user)
                            .setName(frameworkName)
                            .setFailoverTimeout(0)
                            .build()
                    )
            )
            .build();

    }
}
