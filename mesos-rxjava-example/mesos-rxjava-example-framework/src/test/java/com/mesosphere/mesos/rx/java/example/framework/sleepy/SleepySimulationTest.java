/*
 *    Copyright (C) 2016 Mesosphere, Inc
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

package com.mesosphere.mesos.rx.java.example.framework.sleepy;

import com.google.protobuf.ByteString;
import com.mesosphere.mesos.rx.java.protobuf.ProtobufMessageCodecs;
import com.mesosphere.mesos.rx.java.protobuf.SchedulerCalls;
import com.mesosphere.mesos.rx.java.test.Async;
import com.mesosphere.mesos.rx.java.test.simulation.MesosServerSimulation;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.subjects.BehaviorSubject;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.mesosphere.mesos.rx.java.protobuf.SchedulerEvents.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.mesos.v1.Protos.TaskState.TASK_RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

public final class SleepySimulationTest {

    @NotNull
    private static final Protos.Event HEARTBEAT = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.HEARTBEAT)
        .build();

    @Rule
    public Timeout timeoutRule = new Timeout(15_000, MILLISECONDS);

    @Rule
    public Async async = new Async();

    private BehaviorSubject<Protos.Event> subject;
    private MesosServerSimulation<Protos.Event, Protos.Call> sim;

    private URI uri;

    @Before
    public void setUp() throws Exception {
        subject = BehaviorSubject.create();
        sim = new MesosServerSimulation<>(
            subject,
            ProtobufMessageCodecs.SCHEDULER_EVENT,
            ProtobufMessageCodecs.SCHEDULER_CALL,
            (e) -> e.getType() == Protos.Call.Type.SUBSCRIBE
        );
        final int serverPort = sim.start();
        uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
    }

    @After
    public void tearDown() throws Exception {
        sim.shutdown();
    }

    @Test
    public void offerSimulation() throws Throwable {
        final String fwId = "sleepy-" + UUID.randomUUID();
        final org.apache.mesos.v1.Protos.FrameworkID frameworkID = org.apache.mesos.v1.Protos.FrameworkID.newBuilder()
            .setValue(fwId)
            .build();
        final Protos.Call subscribe = SchedulerCalls.subscribe(
            frameworkID,
            org.apache.mesos.v1.Protos.FrameworkInfo.newBuilder()
                .setId(frameworkID)
                .setUser("root")
                .setName("sleepy")
                .setFailoverTimeout(0)
                .setRole("*")
                .build()
        );

        async.run("sleepy-framework", () -> Sleepy._main(fwId, uri.toString(), "1", "*"));

        subject.onNext(subscribed(fwId, 15));
        sim.awaitSubscribeCall();
        final List<Protos.Call> callsReceived1 = sim.getCallsReceived();
        assertThat(callsReceived1).hasSize(1);
        assertThat(callsReceived1.get(0)).isEqualTo(subscribe);

        // send a heartbeat
        subject.onNext(HEARTBEAT);
        // send an offer
        subject.onNext(resourceOffer("host-1", "offer-1", "agent-1", fwId, 4, 16 * 1024, 100 * 1024));

        sim.awaitCall();  // wait for accept to reach the server
        final List<Protos.Call> callsReceived2 = sim.getCallsReceived();
        assertThat(callsReceived2).hasSize(2);
        final Protos.Call.Accept accept = callsReceived2.get(1).getAccept();
        assertThat(accept).isNotNull();
        assertThat(
            accept.getOfferIdsList().stream()
                .map(org.apache.mesos.v1.Protos.OfferID::getValue)
                .collect(Collectors.toList())
        ).isEqualTo(newArrayList("offer-1"));
        final List<org.apache.mesos.v1.Protos.TaskInfo> taskInfos = accept.getOperationsList().stream()
            .map(org.apache.mesos.v1.Protos.Offer.Operation::getLaunch)
            .flatMap(l -> l.getTaskInfosList().stream())
            .collect(Collectors.toList());
        assertThat(taskInfos).hasSize(4);
        assertThat(
            taskInfos.stream()
                .map(t -> t.getTaskId().getValue())
                .collect(Collectors.toSet())
        ).isEqualTo(newHashSet("task-1-1", "task-1-2", "task-1-3", "task-1-4"));

        // send task status updates
        final ByteString uuid1 = ByteString.copyFromUtf8(UUID.randomUUID().toString());
        final ByteString uuid2 = ByteString.copyFromUtf8(UUID.randomUUID().toString());
        final ByteString uuid3 = ByteString.copyFromUtf8(UUID.randomUUID().toString());
        final ByteString uuid4 = ByteString.copyFromUtf8(UUID.randomUUID().toString());
        subject.onNext(update("agent-1", "task-1-1", "task-1-1", TASK_RUNNING, uuid1));
        subject.onNext(update("agent-1", "task-1-2", "task-1-2", TASK_RUNNING, uuid2));
        subject.onNext(update("agent-1", "task-1-3", "task-1-3", TASK_RUNNING, uuid3));
        subject.onNext(update("agent-1", "task-1-4", "task-1-4", TASK_RUNNING, uuid4));

        // wait for the task status updates to be ack'd
        sim.awaitCall(4);
        final List<Protos.Call> callsReceived3 = sim.getCallsReceived();
        assertThat(callsReceived3).hasSize(6);
        final List<Protos.Call> ackCalls = callsReceived3.subList(2, 6);
        final Set<ByteString> ackdUuids = ackCalls.stream()
            .map(c -> c.getAcknowledge().getUuid())
            .collect(Collectors.toSet());

        assertThat(ackdUuids).isEqualTo(newHashSet(uuid1, uuid2, uuid3, uuid4));

        // send another offer with too little cpu for a task to run
        subject.onNext(resourceOffer("host-1", "offer-2", "agent-1", fwId, 0.9, 15 * 1024, 100 * 1024));

        // wait for the decline of the offer
        sim.awaitCall();
        final List<Protos.Call> callsReceived4 = sim.getCallsReceived();
        assertThat(callsReceived4).hasSize(7);
        final Protos.Call.Decline decline = callsReceived4.get(6).getDecline();
        assertThat(
            decline.getOfferIdsList().stream()
                .map(org.apache.mesos.v1.Protos.OfferID::getValue)
                .collect(Collectors.toList())
        ).isEqualTo(newArrayList("offer-2"));

        subject.onCompleted();
        sim.awaitSendingEvents();
    }

}
