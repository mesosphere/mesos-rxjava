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

package com.mesosphere.mesos.rx.java.recordio;

import com.google.common.collect.Lists;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

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
     * <li>{@code heartbeatIntervalSeconds = 15}</li>
     * </ul>
     */
    @NotNull
    public static final Protos.Event SUBSCRIBED = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.SUBSCRIBED)
        .setSubscribed(
            Protos.Event.Subscribed.newBuilder()
                .setFrameworkId(org.apache.mesos.v1.Protos.FrameworkID.newBuilder()
                    .setValue("a7cfd25c-79bd-481c-91cc-692e5db1ec3d")
                )
                .setHeartbeatIntervalSeconds(15)
        )
        .build();

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
    public static final Protos.Event OFFER = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.OFFERS)
        .setOffers(
            Protos.Event.Offers.newBuilder()
                .addAllOffers(Lists.newArrayList(
                    org.apache.mesos.v1.Protos.Offer.newBuilder()
                        .setHostname("host1")
                        .setId(org.apache.mesos.v1.Protos.OfferID.newBuilder().setValue("offer1"))
                        .setAgentId(org.apache.mesos.v1.Protos.AgentID.newBuilder().setValue("agent1"))
                        .setFrameworkId(org.apache.mesos.v1.Protos.FrameworkID.newBuilder().setValue("frw1"))
                        .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                            .setName("cpus")
                            .setRole("*")
                            .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                            .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue(8d)))
                        .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                            .setName("mem")
                            .setRole("*")
                            .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                            .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue((long) 8192)))
                        .addResources(org.apache.mesos.v1.Protos.Resource.newBuilder()
                            .setName("disk")
                            .setRole("*")
                            .setType(org.apache.mesos.v1.Protos.Value.Type.SCALAR)
                            .setScalar(org.apache.mesos.v1.Protos.Value.Scalar.newBuilder().setValue((long) 8192)))
                        .build()
                ))
        )
        .build();

}
