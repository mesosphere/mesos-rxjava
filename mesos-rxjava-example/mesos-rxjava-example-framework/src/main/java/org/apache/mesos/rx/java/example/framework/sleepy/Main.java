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

package org.apache.mesos.rx.java.example.framework.sleepy;

import com.google.common.base.Joiner;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.apache.mesos.rx.java.MesosSchedulerClient;
import org.apache.mesos.rx.java.ProtoUtils;
import org.apache.mesos.v1.Protos.*;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static rx.Observable.from;
import static rx.Observable.just;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final AtomicInteger counter = new AtomicInteger();
    private static final int MEM = 32;

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            final String className = Main.class.getCanonicalName();
            System.err.println("Usage: java -cp <application-jar> " + className + " <host> <port> <cpus-per-task>");
        }

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final double cpusPerTask = Double.parseDouble(args[2]);

        final MesosSchedulerClient<Protos.Call, Protos.Event> client = MesosSchedulerClient.usingProtos(host, port);

        final Call subscribeCall = Call.newBuilder()
            .setType(Call.Type.SUBSCRIBE)
            .setSubscribe(
                Call.Subscribe.newBuilder()
                    .setFrameworkInfo(
                        FrameworkInfo.newBuilder()
                            .setUser(Optional.ofNullable(System.getenv("user")).orElse("root")) // https://issues.apache.org/jira/browse/MESOS-3747
                            .setName("testing")
                            .setFailoverTimeout(0)
                            .build()
                    )
            )
            .build();

        final Observable<Event> events = client.openEventStream(subscribeCall)
            .share();

        final AtomicReference<FrameworkID> reference = new AtomicReference<>(null);
        final Observable<AtomicReference<FrameworkID>> frameworkIDObservable = just(reference).repeat();

        final Subscription setFrameworkIdSubscription = events
            .filter(event -> event.getType() == Event.Type.SUBSCRIBED)
            .zipWith(frameworkIDObservable, (Event e, AtomicReference<FrameworkID> fwIdRef) -> {
                final FrameworkID frameworkId = e.getSubscribed().getFrameworkId();
                LOGGER.info("Setting frameworkId to {}", ProtoUtils.protoToString(frameworkId));
                fwIdRef.set(frameworkId);
                return e;
            })
            .subscribe();

        final Observable<Call> declines = events
            .zipWith(frameworkIDObservable, Tuple2::create)
            .filter((Tuple2<Event, AtomicReference<FrameworkID>> t) -> t._1.getType() == Event.Type.OFFERS && t._2.get() != null)
            .flatMap((Tuple2<Event, AtomicReference<FrameworkID>> t) -> {
                final FrameworkID frameworkId = t._2.get();
                final Event event = t._1;
                final List<Call> calls = event.getOffers().getOffersList()
                    .stream()
                    .flatMap((Offer offer) -> {
                        final AgentID agentId = offer.getAgentId();
                        final Map<String, List<Resource>> resources = offer.getResourcesList()
                            .stream()
                            .collect(groupingBy(Resource::getName));
                        final List<Resource> cpuList = resources.get("cpus");
                        final List<Resource> memList = resources.get("mem");
                        if (cpuList != null && !cpuList.isEmpty() && memList != null && !memList.isEmpty()) {
                            final Resource cpus = cpuList.iterator().next();
                            final Resource mem = memList.iterator().next();
                            final List<TaskInfo> tasks = newArrayList();

                            double availableCpu = cpus.getScalar().getValue();
                            double availableMem = mem.getScalar().getValue();
                            while (availableCpu >= cpusPerTask && availableMem >= MEM) {
                                availableCpu -= cpusPerTask;
                                availableMem -= MEM;
                                tasks.add(sleepTask(agentId, cpusPerTask, MEM));
                            }


                            final List<OfferID> ids = newArrayList(offer.getId());
                            if (!tasks.isEmpty()) {
                                LOGGER.info("Launching {} tasks", tasks.size());
                                return Stream.of(sleep(
                                    frameworkId,
                                    ids,
                                    tasks
                                ));
                            } else {
                                return Stream.of(ProtoUtils.decline(frameworkId, ids));
                            }
                        } else {
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toList());
                return from(calls);
            });

        final Observable<Call> updateStatusAck = events
            .filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().hasUuid())
            .zipWith(frameworkIDObservable, Tuple2::create)
            .map((Tuple2<Event, AtomicReference<FrameworkID>> t) -> {
                final TaskStatus status = t._1.getUpdate().getStatus();
                return ProtoUtils.ackUpdate(t._2.get(), status.getUuid(), status.getAgentId(), status.getTaskId());
            });

        final Observable<Call> calls = declines.mergeWith(updateStatusAck);
        final Observable<?> sink = client.sink(calls)
            .subscribeOn(Schedulers.io())
            .filter(resp -> resp.getStatus().code() != 202)
            .flatMap((failedResponse) ->
                    // TODO: Figure out a better way to communicate this scenario to the application
                    failedResponse.getContent()
                        .map(buf -> {
                            final String message = buf.toString(CharsetUtil.UTF_8);
                            final HttpResponseHeaders headers = failedResponse.getHeaders();
                            final List<Map.Entry<String, String>> entries = headers.entries();
                            final String headersString = Joiner.on(",").skipNulls().join(entries);
                            return Tuple2.t(failedResponse.getStatus(), Tuple2.t(headersString, message));
                        })
            )
            .doOnNext((Tuple2<HttpResponseStatus, Tuple2<String, String>> t) ->
                    LOGGER.warn(
                        "Failed response: Status: {}, Headers: {}, Message: '{}'",
                        t._1,
                        t._2._1,
                        t._2._2
                    )
            );

        final Subscription errorLoggerSubscription = events.
            filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().getState() == TaskState.TASK_ERROR)
            .doOnNext(e -> LOGGER.warn("Task Error: {}", ProtoUtils.protoToString(e)))
            .subscribe();

        sink
            .toBlocking()
            .last();
        setFrameworkIdSubscription.unsubscribe();
        errorLoggerSubscription.unsubscribe();
    }

    @NotNull
    private static Call sleep(@NotNull final FrameworkID frameworkId, final List<OfferID> offerIds, final List<TaskInfo> tasks) {
        return Call.newBuilder()
            .setFrameworkId(frameworkId)
            .setType(Call.Type.ACCEPT)
            .setAccept(
                Call.Accept.newBuilder()
                    .addAllOfferIds(offerIds)
                    .addOperations(
                        Offer.Operation.newBuilder()
                            .setType(Offer.Operation.Type.LAUNCH)
                            .setLaunch(
                                Offer.Operation.Launch.newBuilder()
                                    .addAllTaskInfos(tasks)
                            )
                    )
            )
            .build();
    }

    @NotNull
    private static TaskInfo sleepTask(@NotNull final AgentID agentId, final double cpus, final int mem) {
        final String taskId = String.format("task-%d", counter.incrementAndGet());
        return TaskInfo.newBuilder()
            .setName(taskId)
            .setTaskId(
                TaskID.newBuilder()
                    .setValue(taskId)
            )
            .setAgentId(agentId)
            .setCommand(
                CommandInfo.newBuilder()
                    .setEnvironment(Environment.newBuilder()
                        .addVariables(
                            Environment.Variable.newBuilder()
                                .setName("SLEEP_SECONDS").setValue("60")
                        ))
                    .setValue("env | sort && sleep $SLEEP_SECONDS")
            )
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
            .build();
    }

}
