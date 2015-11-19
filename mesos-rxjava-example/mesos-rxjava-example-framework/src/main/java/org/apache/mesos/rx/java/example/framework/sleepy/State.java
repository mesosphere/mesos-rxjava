package org.apache.mesos.rx.java.example.framework.sleepy;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newConcurrentMap;
import static org.apache.mesos.rx.java.ProtoUtils.protoToString;

final class State<FwId, TaskId, TaskState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    private final double cpusPerTask;
    private final double memMbPerTask;

    @NotNull
    private final Map<TaskId, TaskState> taskStates;
    @NotNull
    private final AtomicInteger offerCounter = new AtomicInteger();
    @NotNull
    private final AtomicInteger totalTaskCounter = new AtomicInteger();

    @NotNull
    private final FwId fwId;

    public State(@NotNull final FwId fwId, final double cpusPerTask, final double memMbPerTask) {
        this.fwId = fwId;
        this.cpusPerTask = cpusPerTask;
        this.memMbPerTask = memMbPerTask;
        this.taskStates = newConcurrentMap();
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
