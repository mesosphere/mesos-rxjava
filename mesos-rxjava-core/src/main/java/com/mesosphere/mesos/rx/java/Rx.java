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

package com.mesosphere.mesos.rx.java;

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
