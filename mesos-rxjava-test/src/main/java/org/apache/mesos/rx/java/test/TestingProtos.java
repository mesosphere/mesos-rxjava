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

public final class TestingProtos {
    @NotNull
    public static final Protos.Event HEARTBEAT = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.HEARTBEAT)
        .build();
    @NotNull
    public static final Protos.Event SUBSCRIBED = subscribed("20151008-161417-16777343-5050-20532-0008", 15);
    @NotNull
    public static final Protos.Event OFFER = resourceOffer("host1", "offer", "slave", "frw1", 8d, 8192, 8192);

    @NotNull
    public static final Protos.Call SUBSCRIBE = subscribe(
        "a7cfd25c-79bd-481c-91cc-692e5db1ec3d", "unit-test-user", "unit-testing"
    );

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
