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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mesosphere.mesos.rx.java.protobuf.ProtoUtils.protoToString;

final class State<FwId, TaskId, TaskState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    private final double cpusPerTask;
    private final double memMbPerTask;
    @NotNull
    private final String resourceRole;

    @NotNull
    private final Map<TaskId, TaskState> taskStates;
    @NotNull
    private final AtomicInteger offerCounter = new AtomicInteger();
    @NotNull
    private final AtomicInteger totalTaskCounter = new AtomicInteger();

    @NotNull
    private final FwId fwId;

    public State(
        @NotNull final FwId fwId,
        @NotNull final String resourceRole,
        final double cpusPerTask,
        final double memMbPerTask
    ) {
        this.fwId = fwId;
        this.resourceRole = resourceRole;
        this.cpusPerTask = cpusPerTask;
        this.memMbPerTask = memMbPerTask;
        this.taskStates = new ConcurrentHashMap<>();
    }

    @NotNull
    public FwId getFwId() {
        return fwId;
    }

    public double getCpusPerTask() {
        return cpusPerTask;
    }

    public double getMemMbPerTask() {
        return memMbPerTask;
    }

    @NotNull
    public String getResourceRole() {
        return resourceRole;
    }

    @NotNull
    public AtomicInteger getOfferCounter() {
        return offerCounter;
    }

    @NotNull
    public AtomicInteger getTotalTaskCounter() {
        return totalTaskCounter;
    }

    public void put(final TaskId key, final TaskState value) {
        LOGGER.debug("put(key : {}, value : {})", protoToString(key), value);
        taskStates.put(key, value);
    }
}
