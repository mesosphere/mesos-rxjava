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

package com.mesosphere.mesos.rx.java.test.simulation;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.util.concurrent.Semaphore;

/**
 * A {@link Subscriber} that will decorate a provided {@code Subscriber} adding the ability for a client to "block"
 * until an event is observed, at which point the client can proceed. This class is really only useful in the context
 * of writing tests.
 * <p>
 * {@code TRACE} level logging can be enable for this class to see exactly what is happening in the subscriber
 * while events are flowing in or while {@link #awaitEvent() awaiting event(s)}.
 * @param <T> The type of event that will be sent into this subscriber.
 */
public final class AwaitableEventSubscriberDecorator<T> extends Subscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwaitableEventSubscriberDecorator.class);

    @NotNull
    private final Subscriber<T> obs;

    @NotNull
    private final Semaphore sem;

    /**
     * Creates the decorator based on wrapping the provided {@code obs}.
     * @param obs    An {@code AwaitableEventSubscriberDecorator} that can be used to wait for event(s)
     */
    public AwaitableEventSubscriberDecorator(@NotNull final Subscriber<T> obs) {
        this.obs = obs;
        this.sem = new Semaphore(0, true);
    }

    @Override
    public void onNext(final T t) {
        LOGGER.trace("onNext");
        obs.onNext(t);
        sem.release();
        LOGGER.trace("--- (queueLength, available) = ({}, {})", sem.getQueueLength(), sem.availablePermits());
    }

    @Override
    public void onError(final Throwable e) {
        LOGGER.trace("onError", e);
        obs.onError(e);
        releaseAll();
        LOGGER.trace("--- (queueLength, available) = ({}, {})", sem.getQueueLength(), sem.availablePermits());
    }

    @Override
    public void onCompleted() {
        LOGGER.trace("onCompleted");
        obs.onCompleted();
        releaseAll();
        LOGGER.trace("--- (queueLength, available) = ({}, {})", sem.getQueueLength(), sem.availablePermits());
    }

    /**
     * Block the invoking thread until {@code 1} event is observed by this instance. Any of {@link #onNext(Object)},
     * {@link #onError(Throwable)} or {@link #onCompleted()} are considered to be events.
     * @throws InterruptedException Throw in the waiting thread is interrupted.
     */
    public void awaitEvent() throws InterruptedException {
        awaitEvent(1);
    }

    /**
     * Block the invoking thread until {@code eventCount} events are observed by this instance. Any of
     * {@link #onNext(Object)}, {@link #onError(Throwable)} or {@link #onCompleted()} are considered to be events.
     * <p>
     * In the case of {@link #onError(Throwable)} or {@link #onCompleted()} being invoked this method will return
     * regardless of the value of {@code eventCount}.
     * @param eventCount The number of events to block and wait for
     * @throws InterruptedException Throw in the waiting thread is interrupted.
     */
    public void awaitEvent(final int eventCount) throws InterruptedException {
        LOGGER.trace("awaitEvent(eventCount : {})", eventCount);
        LOGGER.trace(">>> (queueLength, available) = ({}, {})", sem.getQueueLength(), sem.availablePermits());
        sem.acquire(eventCount);
        LOGGER.trace("<<< (queueLength, available) = ({}, {})", sem.getQueueLength(), sem.availablePermits());
    }

    private void releaseAll() {
        sem.release(Integer.MAX_VALUE - sem.availablePermits());
    }

}
