package org.apache.mesos.rx.java.example.framework.sleepy;

import org.apache.mesos.v1.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newConcurrentMap;

final class State {
    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    private final double cpusPerTask;
    private final double memMbPerTask;

    @NotNull
    private final Map<Protos.TaskID, Protos.TaskState> taskStates;
    @NotNull
    private final AtomicInteger offerCounter = new AtomicInteger();
    @NotNull
    private final AtomicInteger totalTaskCounter = new AtomicInteger();

    @NotNull
    private final Protos.FrameworkID fwId;

    public State(@NotNull final Protos.FrameworkID fwId, final double cpusPerTask, final double memMbPerTask) {
        this.fwId = fwId;
        this.cpusPerTask = cpusPerTask;
        this.memMbPerTask = memMbPerTask;
        this.taskStates = newConcurrentMap();
    }

    @NotNull
    public Protos.FrameworkID getFwId() {
        return fwId;
    }

    public double getCpusPerTask() {
        return cpusPerTask;
    }

    public double getMemMbPerTask() {
        return memMbPerTask;
    }

    @NotNull
    public AtomicInteger getOfferCounter() {
        return offerCounter;
    }

    @NotNull
    public AtomicInteger getTotalTaskCounter() {
        return totalTaskCounter;
    }

    public void put(final Protos.TaskID key, final Protos.TaskState value) {
        LOGGER.debug("put(key : {}, value : {})", key.getValue(), value);
        taskStates.put(key, value);
    }
}
