package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * A set of utilities related to Rx.
 */
public final class Rx {

    private Rx() {}

    /**
     * This method will return the {@link Scheduler} that is used for all "computation" workloads.
     * <p/>
     * The idea of a computation workload is one that is CPU bound.
     * @return The {@link Scheduler} that is to be used for all CPU bound workloads.
     */
    @NotNull
    public static Scheduler compute() {
        return Schedulers.computation();
    }

    /**
     * This method will return the {@link Scheduler} that is used for all "io" workloads.
     * <p/>
     * The idea of a io workload is one that is IO bound (disk, network, etc.).
     * @return The {@link Scheduler} that is to be used for all IO bound workloads.
     */
    @NotNull
    public static Scheduler io() {
        return Schedulers.io();
    }
}
