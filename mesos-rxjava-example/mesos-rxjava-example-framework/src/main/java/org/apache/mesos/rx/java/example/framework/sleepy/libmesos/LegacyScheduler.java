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

package org.apache.mesos.rx.java.example.framework.sleepy.libmesos;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.rx.java.ProtoUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.mesos.Protos.*;
import static org.apache.mesos.rx.java.example.framework.sleepy.Main.CPUS;
import static org.apache.mesos.rx.java.example.framework.sleepy.Main.MEM;

public final class LegacyScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LegacyScheduler.class);

    private static final AtomicInteger counter = new AtomicInteger();

    private FrameworkID frameworkId;

    @Override
    public void registered(final SchedulerDriver driver, final FrameworkID frameworkId, final MasterInfo masterInfo) {
        LOGGER.info(
            "registered(driver : {}, frameworkId : {}, masterInfo : {})",
            driver,
            ProtoUtils.protoToString(frameworkId),
            ProtoUtils.protoToString(masterInfo)
        );
        this.frameworkId = frameworkId;
    }

    @Override
    public void reregistered(final SchedulerDriver driver, final MasterInfo masterInfo) {
        LOGGER.trace("reregistered(driver : {}, masterInfo : {})", driver, masterInfo);
    }

    @Override
    public void resourceOffers(final SchedulerDriver driver, final List<Protos.Offer> offers) {
//        LOGGER.trace("resourceOffers(driver : {}, offers : {})", driver, offers);
        offers.stream()
            .forEach((Offer offer) -> {
                final OfferID offerId = offer.getId();
                final SlaveID slaveId = offer.getSlaveId();
                final Map<String, List<org.apache.mesos.Protos.Resource>> resources = offer.getResourcesList()
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
                    while (availableCpu >= CPUS && availableMem >= MEM) {
                        availableCpu -= CPUS;
                        availableMem -= MEM;
                        tasks.add(sleepTask(slaveId));
                    }


                    final List<OfferID> ids = newArrayList(offerId);
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
        LOGGER.trace("offerRescinded(driver : {}, offerId : {})", driver, offerId);
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
        LOGGER.trace("statusUpdate(driver : {}, status : {})", driver, status);
    }

    @Override
    public void frameworkMessage(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
        LOGGER.trace("frameworkMessage(driver : {}, executorId : {}, slaveId : {}, data : {})", driver, executorId, slaveId, data);
    }

    @Override
    public void disconnected(final SchedulerDriver driver) {
        LOGGER.trace("disconnected(driver : {})", driver);
    }

    @Override
    public void slaveLost(final SchedulerDriver driver, final SlaveID slaveId) {
        LOGGER.trace("slaveLost(driver : {}, slaveId : {})", driver, slaveId);
    }

    @Override
    public void executorLost(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
        LOGGER.trace("executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, executorId, slaveId, status);
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.trace("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    private static TaskInfo sleepTask(@NotNull final SlaveID slaveId) {
        final String taskId = String.format("task-%d", counter.incrementAndGet());
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
                                .setName("SLEEP_SECONDS").setValue("60")
                        ))
                    .setValue("env | sort && sleep $SLEEP_SECONDS")
            )
            .addResources(Resource.newBuilder()
                .setName("cpus")
                .setRole("*")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder().setValue(CPUS)))
            .addResources(Resource.newBuilder()
                .setName("mem")
                .setRole("*")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder().setValue(MEM)))
            .build();
    }

}
