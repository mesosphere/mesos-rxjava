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
 *
 */
/**
 * This package provides the code necessary to create and interact with {@link rx.Observable} events
 * coming from Apache Mesos.
 *
 * The following is a full Apache Mesos Framework that declines all resource offers:
 * <pre><code>
 * import static org.apache.mesos.v1.Protos.*;
 * import static org.apache.mesos.v1.scheduler.Protos.*;
 *
 * final Protos.FrameworkID fwId = FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
 * org.apache.mesos.rx.java.MesosSchedulerClientBuilders.usingProtos()
 *     .mesosUri(URI.create("http://localhost:5050/api/v1/scheduler"))
 *     .applicationUserAgentEntry(UserAgentEntries.literal("decline-framework", "0.1.0-SNAPSHOT"))
 *     .subscribe(
 *         Call.newBuilder()
 *             .setFrameworkId(fwId)
 *             .setType(Call.Type.SUBSCRIBE)
 *             .setSubscribe(
 *                 Call.Subscribe.newBuilder()
 *                     .setFrameworkInfo(
 *                         Protos.FrameworkInfo.newBuilder()
 *                             .setId(fwId)
 *                             .setUser("root")
 *                             .setName("decline-framework")
 *                             .setFailoverTimeout(0)
 *                     )
 *             )
 *             .build()
 *     )
 *     .processStream(es ->
 *         es.filter(event -> event.getType() == Event.Type.OFFERS)
 *         .flatMap(event -> Observable.from(event.getOffers().getOffersList()))
 *         .map(offer ->
 *             Call.newBuilder()
 *                 .setFrameworkId(fwId)
 *                 .setType(Call.Type.DECLINE)
 *                 .setDecline(
 *                     Call.Decline.newBuilder()
 *                         .addOfferIds(offer.getId())
 *                 )
 *                 .build()
 *         )
 *         .map(SinkOperations::create)
 *         .map(Optional::of)
 *     )
 *     .build()
 *     .openStream()
 *     .await();
 * </code></pre>
 */
package org.apache.mesos.rx.java;
