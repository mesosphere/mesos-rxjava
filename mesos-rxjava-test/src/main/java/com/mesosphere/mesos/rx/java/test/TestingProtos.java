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

package com.mesosphere.mesos.rx.java.test;

import com.google.common.collect.Lists;
import com.mesosphere.mesos.rx.java.util.SchedulerCalls;
import com.mesosphere.mesos.rx.java.util.SchedulerEvents;
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
     * <li>{@code hearbeatIntervalSeconds = 15}</li>
     * </ul>
     */
    @NotNull
    public static final Protos.Event SUBSCRIBED = SchedulerEvents.subscribed("a7cfd25c-79bd-481c-91cc-692e5db1ec3d", 15);

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
    public static final Protos.Event OFFER = SchedulerEvents.resourceOffer("host1", "offer1", "agent1", "frw1", 8d, 8192, 8192);

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
    public static final Protos.Call SUBSCRIBE = SchedulerCalls.subscribe(
        "a7cfd25c-79bd-481c-91cc-692e5db1ec3d", "unit-test-user", "unit-testing", 0
    );

    @NotNull
    public static final Protos.Call DECLINE_OFFER = SchedulerCalls.decline(
        SUBSCRIBE.getFrameworkId(), Lists.newArrayList(OFFER.getOffers().getOffersList().iterator().next().getId())
    );

}
