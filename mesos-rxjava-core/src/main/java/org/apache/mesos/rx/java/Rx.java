package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import rx.Scheduler;
import rx.schedulers.Schedulers;

public final class Rx {

    private Rx() {}

    @NotNull
    public static Scheduler compute() {
        return Schedulers.computation();
    }

    @NotNull
    public static Scheduler io() {
        return Schedulers.io();
    }
}
