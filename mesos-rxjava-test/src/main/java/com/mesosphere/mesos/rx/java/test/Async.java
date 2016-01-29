/*
 *    Copyright (C) 2016 Mesosphere, Inc
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

package com.mesosphere.mesos.rx.java.test;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.Verifier;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;

/**
 * A JUnit {@link org.junit.rules.TestRule Rule} that provides a convenience method to run async tasks.
 * <p>
 * Async tasks that are run will be verfied to have completed at the end of the test run, if not an
 * {@link AssertionError} will be throw.
 * <p>
 * If an {@link AssertionError} is the result of the async task (for example doing an assertion as part of the
 * task), that {@code AssertionError} will be rethrown.
 *
 * <pre>
 * public class TestClass {
 *
 *    &#064;Rule
 *    public Async async = new Async();
 *
 *    &#064;Test
 *    public void testWithAsyncWork() {
 *       async.run(() -&gt;
 *          assertTrue(true)
 *       );
 *    }
 * }
 * </pre>
 */
public final class Async extends Verifier {

    @NotNull
    private final AtomicInteger counter;

    @NotNull
    private final ExecutorService executor;

    @NotNull
    private final List<Task> tasks;

    public Async() {
        counter = new AtomicInteger(0);
        executor = Executors.newCachedThreadPool(new DefaultThreadFactory(Async.class));
        tasks = Collections.synchronizedList(newArrayList());
    }

    /**
     * Run {@code r} on a background CachedThreadPool
     * @param r    The task to run
     */
    public void run(@NotNull final ErrorableRunnable r) {
        run(String.format("Async-%d", counter.getAndIncrement()), r);
    }

    /**
     * Run {@code r} on a background CachedThreadPool
     * @param taskName  The name of the task (used for error messages)
     * @param r         The task to run
     */
    public void run(@NotNull final String taskName, @NotNull final ErrorableRunnable r) {
        tasks.add(new Task(taskName, executor.submit(r)));
    }

    @Override
    protected void verify() throws Throwable {
        for (Task task : tasks) {
            try {
                task.future.get(10, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause != null && cause instanceof AssertionError) {
                    throw cause;
                }
                throw new AssertionError("Error while running Async", cause);
            } catch (TimeoutException te) {
                throw new AssertionError(String.format("Task [%s] still running after test completion", task.name));
            }
        }
    }

    /**
     * Convenience type that allows submitting a runnable that may throw a checked exception.
     * <p>
     * If a checked exception is throw while running the task it will be wrapped in a {@link RuntimeException} and
     * rethrown. If an {@link AssertionError} or {@link RuntimeException} is throw it will be rethrown without being
     * wrapped.
     */
    @FunctionalInterface
    public interface ErrorableRunnable extends Runnable {
        void invoke() throws Throwable;
        default void run() {
            try {
                invoke();
            } catch(AssertionError | RuntimeException e) {
                throw e;
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    private static final class Task {
        @NotNull
        private final Future<?> future;
        @NotNull
        private final String name;

        public Task(@NotNull final String name, @NotNull final Future<?> future) {
            this.future = future;
            this.name = name;
        }
    }
}
