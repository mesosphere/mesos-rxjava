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

import com.mesosphere.mesos.rx.java.test.RecordIOUtils;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.server.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;
import rx.subscriptions.MultipleAssignmentSubscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * {@code MesosServerSimulation} provides a server implementing the same protocol defined by Apache Mesos for its
 * <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md">HTTP Scheduler API</a>.
 * The server has the following behavior:
 * <ol>
 * <li>Run on a random port.</li>
 * <li>Only one HTTP endpoint {@code /api/v1/scheduler}.</li>
 * <li>Only supported method is {@code POST}.</li>
 * <li>All requests sent to the server must specify {@code Content-Length} header.</li>
 * <li>All messages sent to the server are interpreted as {@link Call}s.</li>
 * <li>All messages sent from the server are {@link Event}s.</li>
 * <li>Authentication and Authorization are completely ignored.</li>
 * <li>The events to be sent by the server are represented by the {@link Observable} passed to the constructor.</li>
 * <li>Server only supports one event stream, once that stream is complete a new server will need to be created.</li>
 * </ol>
 */
public final class MesosServerSimulation<Event, Call> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosServerSimulation.class);
    private static final Marker RECEIVE_MARKER = MarkerFactory.getMarker("<<<");
    private static final Marker SEND_MARKER = MarkerFactory.getMarker(">>>");

    @NotNull
    private final List<Call> callsReceived;

    @NotNull
    private final HttpServer<ByteBuf, ByteBuf> server;

    @NotNull
    private final AtomicBoolean started;

    @NotNull
    private final CountDownLatch eventsCompletedLatch;

    @NotNull
    private final CountDownLatch subscribedLatch;

    @NotNull
    private Semaphore sem;

    /**
     * Create a {@code MesosServerSimulation} that will use {@code events} as the event stream to return to a
     * a client when {@code isSubscribePredicate} evaluates to {@code true}
     * <p>
     * The simulation server must be started using {@link #start()} before requests can be serviced by the server.
     *
     * @param events               The event stream to be returned by the server upon {@code isSubscribePredicate}
     *                             evaluating to {@code true} For each {@link Event} sent to {@code events}, the event
     *                             will be sent by the server.
     * @param sendCodec            The {@link MessageCodec} to use to encode {@link Event}s sent by the server
     * @param receiveCodec         The {@link MessageCodec} to use to decode {@link Call}s received by the server
     * @param isSubscribePredicate The predicate used to determine if a {@link Call} is a "Subscribe" call
     */
    public MesosServerSimulation(
        @NotNull final Observable<Event> events,
        @NotNull final MessageCodec<Event> sendCodec,
        @NotNull final MessageCodec<Call> receiveCodec,
        @NotNull final Predicate<Call> isSubscribePredicate
    ) {
        this.callsReceived = Collections.synchronizedList(new ArrayList<>());
        this.started = new AtomicBoolean(false);
        this.eventsCompletedLatch = new CountDownLatch(1);
        this.subscribedLatch = new CountDownLatch(1);
        this.sem = new Semaphore(0);
        this.server = RxNetty.createHttpServer(0, (request, response) -> {
            response.getHeaders().setHeader("Accept", receiveCodec.mediaType());

            if (!"/api/v1/scheduler".equals(request.getUri())) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                response.getHeaders().setHeader("Content-Length", "0");
                return response.close();
            }
            if (!HttpMethod.POST.equals(request.getHttpMethod())
                || !receiveCodec.mediaType().equals(request.getHeaders().getHeader("Content-Type"))
                || request.getHeaders().getContentLength() <= 0
                ) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                response.getHeaders().setHeader("Content-Length", "0");
                return response.close();
            }

            return request.getContent().flatMap(buf -> {
                final ByteBufInputStream in = new ByteBufInputStream(buf);
                final Call call = receiveCodec.decode(in);
                if (callsReceived.add(call)) {
                    sem.release();
                }
                LOGGER.debug(RECEIVE_MARKER, "Call: {}", receiveCodec.show(call));
                if (isSubscribePredicate.test(call)) {
                    if (subscribedLatch.getCount() == 0) {
                        final String message = "Only one event stream can be open per server";
                        response.setStatus(HttpResponseStatus.CONFLICT);
                        response.getHeaders().set("Content-Type", "test/plain;charset=utf-8");
                        response.writeString(message);
                        return response.close();
                    }
                    LOGGER.debug("Responding with event stream from source: {}", events);
                    response.getHeaders().setTransferEncodingChunked();
                    response.getHeaders().set("Content-Type", sendCodec.mediaType());
                    response.getHeaders().add("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
                    response.getHeaders().add("Pragma", "no-cache");

                    final Subject<Void, Void> subject = PublishSubject.create();
                    final MultipleAssignmentSubscription subscription = new MultipleAssignmentSubscription();
                    final Subscription actionSubscription = events
                        .doOnSubscribe(() -> LOGGER.debug("Event stream subscription active"))
                        .doOnNext(e -> LOGGER.debug(SEND_MARKER, "Event: {}", sendCodec.show(e)))
                        .doOnError((t) -> LOGGER.error("Error while creating response", t))
                        .doOnCompleted(() -> {
                            eventsCompletedLatch.countDown();
                            LOGGER.debug("Sending events complete");
                            if (!response.isCloseIssued()) {
                                response.close(true);
                            }
                        })
                        .map(sendCodec::encode)
                        .map(RecordIOUtils::createChunk)
                        .subscribe(bytes -> {
                            if (!response.getChannel().isOpen()) {
                                subscription.unsubscribe();
                                return;
                            }
                            try {
                                LOGGER.trace(SEND_MARKER, "bytes: {}", Arrays.toString(bytes));
                                response.writeBytesAndFlush(bytes);
                            } catch (Exception e) {
                                subject.onError(e);
                            }
                        });
                    subscription.set(actionSubscription);
                    subscribedLatch.countDown();
                    return subject;
                } else {
                    response.setStatus(HttpResponseStatus.ACCEPTED);
                    return response.close();
                }
            });
        });
    }

    /**
     * An unmodifiable list of all {@link Call}s received by the server up to this point.
     *
     * @return An unmodifiable list of all {@link Call}s received by the server up to this point.
     */
    @NotNull
    public List<Call> getCallsReceived() {
        return Collections.unmodifiableList(new ArrayList<>(callsReceived));
    }

    /**
     * Start the server and return the port that the server bound to.
     *
     * @return The port the server bound to
     */
    public int start() {
        started.compareAndSet(false, true);
        server.start();
        return server.getServerPort();
    }

    /**
     * The port the server bound to.
     *
     * @return The port the server bound to.
     */
    public int getServerPort() {
        if (started.get()) {
            return server.getServerPort();
        } else {
            throw new IllegalStateException("Server must be started before attempting to get its port");
        }
    }

    /**
     * Convenience method that can be used to block a thread to wait for all events to be sent from the server.
     * <p>
     * This method will block until {@code onCompleted} is invoked on the {@link Observable} passed to the constructor.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @see CountDownLatch#await()
     */
    public void awaitSendingEvents() throws InterruptedException {
        eventsCompletedLatch.await();
    }

    /**
     * Convenience method that can be used to block a thread to wait for all events to be sent from the server for
     * a configured timeout.
     * <p>
     * This method will block until {@code onCompleted} is invoked on the {@link Observable} passed to the constructor.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return true if the count reached zero and false if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @see CountDownLatch#await(long, TimeUnit)
     */
    public boolean awaitSendingEvents(final long timeout, final TimeUnit unit) throws InterruptedException {
        return eventsCompletedLatch.await(timeout, unit);
    }

    /**
     * Shutdown the server
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void shutdown() throws InterruptedException {
        started.compareAndSet(true, false);
        server.shutdown();
    }

    /**
     * Block the invoking thread until a {@link Call} is received by the server.
     * <p>
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitCall() throws InterruptedException {
        awaitCall(1);
    }

    /**
     * Block the invoking thread until {@code callCount} {@link Call}s are received by the server.
     * <p>
     *
     * @param callCount The number of events to block and wait for
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitCall(final int callCount) throws InterruptedException {
        sem.acquire(callCount);
    }

    /**
     * Block the invoking thread until {@code callCount} {@link Call}s are received by the server or
     * until a specified amount of time elapses.
     * <p>
     *
     * @param callCount The number of events to block and wait for
     * @param timeout The amount of time to wait for the events before timing out
     * @param unit The {@link TimeUnit} for the timeout
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitCall(final int callCount, final long timeout, final TimeUnit unit) throws InterruptedException {
        sem.tryAcquire(callCount, timeout, unit);
    }

    /**
     * Block the invoking thread until a "Subscribe call" is received by the server as determined by the
     * {@code isSubscribePredicate} provided to the constructor.
     * <p>
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitSubscribeCall() throws InterruptedException {
        awaitCall();
        subscribedLatch.await();
    }

    /**
     * Block the invoking thread until a "Subscribe call" is received by the server as determined by the
     * {@code isSubscribePredicate} provided to the constructor.
     * <p>
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return true if the count reached zero and false if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @see CountDownLatch#await(long, TimeUnit)
     */
    public boolean awaitSubscribeCall(final long timeout, final TimeUnit unit) throws InterruptedException {
        return subscribedLatch.await(timeout, unit);
    }
}
