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

package com.mesosphere.mesos.rx.java.example.framework.sleepy;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import com.mesosphere.mesos.rx.java.util.ProtoUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.mesos.Protos.*;

/**
 * A relatively simple Mesos framework that launches {@code sleep $SLEEP_SECONDS} tasks for offers it receives.
 * This framework uses the legacy libmesos API.
 */
public final class LegacySleepy implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LegacySleepy.class);

    /**
     * <pre>{@code
     * Usage: java -cp <application-jar> com.mesosphere.mesos.rx.java.example.framework.sleepy.LegacySleepy <mesos-zk-uri> <cpus-per-task>
     * mesos-zk-uri     The fully qualified URI to the Mesos Master. (zk://localhost:2181/mesos)
     * cpus-per-task    The number of CPUs each task should claim from an offer.
     * }</pre>
     * @param args    Application arguments mesos-uri and cpus-per-task.
     */
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                final String className = Sleepy.class.getCanonicalName();
                System.err.println("Usage: java -cp <application-jar> " + className + " <mesos-zk-uri> <cpus-per-task>");
            }

            final String mesosUri = args[0];
            final double cpusPerTask = Double.parseDouble(args[1]);
            final FrameworkID fwId = FrameworkID.newBuilder().setValue("legacy-sleepy-" + UUID.randomUUID()).build();
            final State<FrameworkID, TaskID, TaskState> state = new State<>(fwId, cpusPerTask, 32);

            final Scheduler scheduler = new LegacySleepy(state);
            final FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder()
                .setUser("")
                .setName("legacy-sleepy")
                .setFailoverTimeout(0)
                .build();

            final MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkInfo, mesosUri);

            driver.run();
        } catch (Throwable e) {
            LOGGER.error("Unhandled exception caught at main", e);
            System.exit(1);
        }
    }

    @NotNull
    private final State<FrameworkID, TaskID, TaskState> state;

    public LegacySleepy(@NotNull final State<FrameworkID, TaskID, TaskState> state) {
        this.state = state;
    }

    @Override
    public void registered(final SchedulerDriver driver, final FrameworkID frameworkId, final MasterInfo masterInfo) {
    }

    @Override
    public void reregistered(final SchedulerDriver driver, final MasterInfo masterInfo) {
    }

    @Override
    public void resourceOffers(final SchedulerDriver driver, final List<Protos.Offer> offers) {
        offers.stream()
            .forEach((Offer offer) -> {
                final int offerCount = state.getOfferCounter().incrementAndGet();

                final SlaveID slaveId = offer.getSlaveId();
                final OfferID offerId = offer.getId();
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
                        tasks.add(sleepTask(slaveId, taskId, cpusPerTask, memMbPerTask));
                    }

                    if (!tasks.isEmpty()) {
                        LOGGER.info("Launching {} tasks", tasks.size());
                        driver.launchTasks(ids, tasks);
                    } else {
                        driver.declineOffer(offerId);
                    }
                } else {
                    driver.declineOffer(offerId);
                }
            });
    }

    @Override
    public void offerRescinded(final SchedulerDriver driver, final OfferID offerId) {
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
        if (status.getState() == TaskState.TASK_ERROR) {
            LOGGER.warn("Task Error: {}", ProtoUtils.protoToString(status));
        }
        state.put(status.getTaskId(), status.getState());
    }

    @Override
    public void frameworkMessage(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
    }

    @Override
    public void disconnected(final SchedulerDriver driver) {
    }

    @Override
    public void slaveLost(final SchedulerDriver driver, final SlaveID slaveId) {
    }

    @Override
    public void executorLost(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
    }

    @NotNull
    private static TaskInfo sleepTask(@NotNull final SlaveID slaveId, @NotNull final String taskId, final double cpus, final double mem) {
        final String sleepSeconds = Optional.ofNullable(System.getenv("SLEEP_SECONDS")).orElse("15");
        return TaskInfo.newBuilder()
            .setName(taskId)
            .setTaskId(
                TaskID.newBuilder()
                    .setValue(taskId)
            )
            .setSlaveId(slaveId)
            .setCommand(
                CommandInfo.newBuilder()
                    .setEnvironment(Environment.newBuilder()
                        .addVariables(
                            Environment.Variable.newBuilder()
                                .setName("SLEEP_SECONDS").setValue(sleepSeconds)
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
