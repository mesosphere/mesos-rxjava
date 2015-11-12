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

import org.apache.mesos.rx.java.*;
import org.apache.mesos.v1.Protos.*;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.mesos.rx.java.ProtoUtils.decline;
import static org.apache.mesos.rx.java.SinkOperations.sink;
import static rx.Observable.from;
import static rx.Observable.just;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String[] args) {
        try {
            if (args.length != 3) {
                final String className = Main.class.getCanonicalName();
                System.err.println("Usage: java -cp <application-jar> " + className + " <host> <port> <cpus-per-task>");
            }

            final String host = args[0];
            final int port = Integer.parseInt(args[1]);
            final double cpusPerTask = Double.parseDouble(args[2]);
            final FrameworkID fwId = FrameworkID.newBuilder().setValue("testing-" + UUID.randomUUID()).build();
            final State state = new State(fwId, cpusPerTask, 32);

            final MesosSchedulerClient<Call, Event> client = MesosSchedulerClient.usingProtos(host, port);
            _main(state, client);
        } catch (Exception e) {
            LOGGER.error("Unhandled exception caught at main", e);
            System.exit(1);
        }
    }

    private static void _main(@NotNull final State stateObject, @NotNull final MesosSchedulerClient<Call, Event> client) throws InterruptedException {

        final Call subscribeCall = Call.newBuilder()
            .setFrameworkId(stateObject.getFwId())
            .setType(Call.Type.SUBSCRIBE)
            .setSubscribe(
                Call.Subscribe.newBuilder()
                    .setFrameworkInfo(
                        FrameworkInfo.newBuilder()
                            .setId(stateObject.getFwId())
                            .setUser(Optional.ofNullable(System.getenv("user")).orElse("root")) // https://issues.apache.org/jira/browse/MESOS-3747
                            .setName("testing")
                            .setFailoverTimeout(0)
                            .build()
                    )
            )
            .build();

        final Observable<Event> events = client.openEventStream(subscribeCall)
            .share()
            .observeOn(Rx.compute());

        final Observable<State> stateObservable = just(stateObject).repeat();

        final Observable<SinkOperation<Call>> offerEvaluations = events
            .filter(event -> event.getType() == Event.Type.OFFERS)
            .flatMap(event -> from(event.getOffers().getOffersList()))
            .zipWith(stateObservable, Tuple2::create)
            .map(Main::handleOffer);

        final Observable<SinkOperation<Call>> updateStatusAck = events
            .filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().hasUuid())
            .zipWith(stateObservable, Tuple2::create)
            .doOnNext((Tuple2<Event, State> t) -> {
                final Event event = t._1;
                final State state = t._2;
                final TaskStatus status = event.getUpdate().getStatus();
                state.put(status.getTaskId(), status.getState());
            })
            .map((Tuple2<Event, State> t) -> {
                final TaskStatus status = t._1.getUpdate().getStatus();
                return ProtoUtils.ackUpdate(t._2.getFwId(), status.getUuid(), status.getAgentId(), status.getTaskId());
            })
            .map(SinkOperations::create);

        final Observable<SinkOperation<Call>> calls = offerEvaluations.mergeWith(updateStatusAck);

        final Subscription errorLoggerSubscription = events
            .filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().getState() == TaskState.TASK_ERROR)
            .doOnNext(e -> LOGGER.warn("Task Error: {}", ProtoUtils.protoToString(e)))
            .subscribe();

        final Subscription sink = client.sink(calls);
        Thread.sleep(Integer.MAX_VALUE); //TODO: Figure out something better than this
        sink.unsubscribe();
        errorLoggerSubscription.unsubscribe();
    }

    @NotNull
    private static SinkOperation<Call> handleOffer(@NotNull final Tuple2<Offer, State> t) {
        final Offer offer = t._1;
        final State state = t._2;
        final int offerCount = state.getOfferCounter().incrementAndGet();

        final FrameworkID frameworkId = state.getFwId();
        final AgentID agentId = offer.getAgentId();
        final List<OfferID> ids = newArrayList(offer.getId());

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
            final double cpusPerTask = state.getCpusPerTask();
            final double memMbPerTask = state.getMemMbPerTask();
            while (availableCpu >= cpusPerTask && availableMem >= memMbPerTask) {
                availableCpu -= cpusPerTask;
                availableMem -= memMbPerTask;
                final String taskId = String.format("task-%d-%d", offerCount, state.getTotalTaskCounter().incrementAndGet());
                tasks.add(sleepTask(agentId, taskId, cpusPerTask, memMbPerTask));
            }

            if (!tasks.isEmpty()) {
                LOGGER.info("Launching {} tasks", tasks.size());
                return sink(
                    sleep(frameworkId, ids, tasks),
                    () -> tasks.forEach(task -> state.put(task.getTaskId(), TaskState.TASK_STAGING))
                );
            } else {
                return sink(decline(frameworkId, ids));
            }
        } else {
            return sink(decline(frameworkId, ids));
        }
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
    private static TaskInfo sleepTask(@NotNull final AgentID agentId, @NotNull final String taskId, final double cpus, final double mem) {
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
                                .setName("SLEEP_SECONDS").setValue("15")
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
