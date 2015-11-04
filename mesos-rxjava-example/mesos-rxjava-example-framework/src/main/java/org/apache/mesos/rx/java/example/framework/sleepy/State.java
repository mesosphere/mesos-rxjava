package org.apache.mesos.rx.java.example.framework.sleepy;

import org.apache.mesos.v1.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.collect.Maps.newConcurrentMap;

final class State {
    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    private Protos.FrameworkID fwId;

    @NotNull
    private final Map<Protos.TaskID, Protos.TaskState> taskStates;

    public State() {
        this.taskStates = newConcurrentMap();
    }

    public synchronized Protos.FrameworkID getFwId() {
        return fwId;
    }

    public synchronized void setFwId(final Protos.FrameworkID fwId) {
        this.fwId = fwId;
    }

    public void put(final Protos.TaskID key, final Protos.TaskState value) {
        LOGGER.debug("put(key : {}, value : {})", key.getValue(), value);
        taskStates.put(key, value);
    }
}
