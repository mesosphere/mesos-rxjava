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

import com.mesosphere.mesos.rx.java.util.MessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgentEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.BackpressureOverflow;
import rx.Observable;
import rx.functions.Action0;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mesosphere.mesos.rx.java.util.Validations.checkNotNull;

/**
 * Builder used to create a {@link MesosClient}.
 * <p>
 * PLEASE NOTE: All methods in this class function as "set" rather than "copy with new value"
 * @param <Send>       The type of objects that will be sent to Mesos
 * @param <Receive>    The type of objects that are expected from Mesos
 */
public final class MesosClientBuilder<Send, Receive> {

    private URI mesosUri;
    private Function<Class<?>, UserAgentEntry> applicationUserAgentEntry;
    private MessageCodec<Send> sendCodec;
    private MessageCodec<Receive> receiveCodec;
    private Send subscribe;
    private Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor;
    private Observable.Transformer<byte[], byte[]> backpressureTransformer;
    private Supplier<Map<String, String>> headerSupplier;

    private MesosClientBuilder() {
        backpressureTransformer = observable -> observable;
    }

    /**
     * Create a new instance of MesosClientBuilder
     * @param <Send>       The type of objects that will be sent to Mesos
     * @param <Receive>    The type of objects are expected from Mesos
     * @return A new instance of MesosClientBuilder
     */
    @NotNull
    public static <Send, Receive> MesosClientBuilder<Send, Receive> newBuilder() {
        return new MesosClientBuilder<>();
    }

    /**
     * The {@link URI} that should be used to connect to Mesos. The following segments of the URI are used:
     * <ul>
     *     <li>hostname</li>
     *     <li>port</li>
     *     <li>username</li>
     *     <li>password</li>
     *     <li>path</li>
     * </ul>
     * @param mesosUri    Fully qualified URI to use to connect to mesos.
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> mesosUri(
        @NotNull final URI mesosUri
    ) {
        this.mesosUri = mesosUri;
        return this;
    }

    /**
     * Sets the function used to create a {@link UserAgentEntry} to be included in the {@code User-Agent} header
     * sent to Mesos for all requests.
     * @param applicationUserAgentEntry    Function to provide the {@link UserAgentEntry}
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> applicationUserAgentEntry(
        @NotNull final Function<Class<?>, UserAgentEntry> applicationUserAgentEntry
    ) {
        this.applicationUserAgentEntry = applicationUserAgentEntry;
        return this;
    }

    /**
     * Allows configuration of the codec used for the {@code Send} type.
     * @param sendCodec    {@link MessageCodec} for {@code Send} type
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> sendCodec(
        @NotNull final MessageCodec<Send> sendCodec
    ) {
        this.sendCodec = sendCodec;
        return this;
    }

    /**
     * Allows configuration of the codec used for the {@code Receive} type.
     * @param receiveCodec    {@link MessageCodec} for {@code Receive} type
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> receiveCodec(
        @NotNull final MessageCodec<Receive> receiveCodec
    ) {
        this.receiveCodec = receiveCodec;
        return this;
    }

    /**
     * @param subscribe     The {@code SUBSCRIBE} to be sent to Mesos when opening the event stream.
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> subscribe(
        @NotNull final Send subscribe
    ) {
        this.subscribe = subscribe;
        return this;
    }

    /**
     * This method provides the means for a user to define how the event stream will be processed.
     * <p>
     * The function passed to this method will function as the actual event processing code for the user.
     * <p>
     * The stream the user will source from is an {@link Observable} of {@code Receive}s. With this stream
     * source a number of functions can be applied to transform/interact/evaluate the stream.
     * <p>
     * The output of this function is the users reaction to each event represented as an
     * {@code Observable<Optional<SinkOperation<Send>>>}. If {@link Optional#isPresent()} the specified
     * {@link SinkOperation} will be processed.
     * <p>
     * For example, if you wanted to log all tasks that result in error:
     * <pre>{@code
     * events -> {
     *     final Observable<Optional<SinkOperation<Call>>> errorLogger = events
     *         .filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().getState() == TaskState.TASK_ERROR)
     *         .doOnNext(e -> LOGGER.warn("Task Error: {}", ProtoUtils.protoToString(e)))
     *         .map(e -> Optional.empty());
     *
     *     return errorLogger;
     * }
     * }</pre>
     * @param streamProcessing    The function that will be woven between the event spout and the call sink
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> processStream(
        @NotNull final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessing
    ) {
        this.streamProcessor = streamProcessing;
        return this;
    }

    /**
     * Instructs the HTTP byte[] stream to be composed with reactive pull backpressure such that
     * a burst of incoming Mesos messages is handled by an unbounded buffer rather than a
     * MissingBackpressureException.
     *
     * As an example, this may be necessary for Mesos schedulers that launch large numbers
     * of tasks at a time and then request reconciliation.
     *
     * @return this builder (allowing for further chained calls)
     * @see <a href="http://reactivex.io/documentation/operators/backpressure.html">ReactiveX operators documentation: backpressure operators</a>
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> onBackpressureBuffer(
    ) {
        this.backpressureTransformer = observable -> observable.onBackpressureBuffer();
        return this;
    }


    /**
     * Instructs the HTTP byte[] stream to be composed with reactive pull backpressure such that
     * a burst of incoming Mesos messages is handled by a bounded buffer rather than a
     * MissingBackpressureException. If the buffer is overflown, a {@link java.nio.BufferOverflowException}
     * is thrown.
     *
     * As an example, this may be necessary for Mesos schedulers that launch large numbers
     * of tasks at a time and then request reconciliation.
     *
     * @param capacity number of slots available in the buffer.
     * @return this builder (allowing for further chained calls)
     * @see <a href="http://reactivex.io/documentation/operators/backpressure.html">ReactiveX operators documentation: backpressure operators</a>
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> onBackpressureBuffer(
        final long capacity
    ) {
        this.backpressureTransformer = observable -> observable.onBackpressureBuffer(capacity);
        return this;
    }


    /**
     * Provides a supplier that will be called to supply arbitrary HTTP headers.
     *
     * @param headerSupplier supplier callback for arbitrary HTTP request headers
     * @return this builder (allowing for further chained calls)
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> headerSupplier(
            @NotNull final Supplier<Map<String, String>> headerSupplier
            ) {
        this.headerSupplier = headerSupplier;
        return this;
    }

    /**
     * Instructs the HTTP byte[] stream to be composed with reactive pull backpressure such that
     * a burst of incoming Mesos messages is handled by a bounded buffer rather than a
     * MissingBackpressureException. If the buffer is overflown, your own custom onOverflow callback
     * will be invoked, and the overflow will mitigate the issue based on the {@link BackpressureOverflow.Strategy}
     * that you select.
     *
     * <ul>
     *     <li>{@link BackpressureOverflow#ON_OVERFLOW_ERROR} (default) will {@code onError} dropping all undelivered items,
     *     unsubscribing from the source, and notifying the producer with {@code onOverflow}. </li>
     *     <li>{@link BackpressureOverflow#ON_OVERFLOW_DROP_LATEST} will drop any new items emitted by the producer while
     *     the buffer is full, without generating any {@code onError}.  Each drop will however invoke {@code onOverflow}
     *     to signal the overflow to the producer.</li>
     *     <li>{@link BackpressureOverflow#ON_OVERFLOW_DROP_OLDEST} will drop the oldest items in the buffer in order to make
     *     room for newly emitted ones. Overflow will not generate an{@code onError}, but each drop will invoke
     *     {@code onOverflow} to signal the overflow to the producer.</li>
     * </ul>
     *
     * As an example, this may be necessary for Mesos schedulers that launch large numbers
     * of tasks at a time and then request reconciliation.
     *
     * @param capacity number of slots available in the buffer.
     * @param onOverflow action to execute if an item needs to be buffered, but there are no available slots.  Null is allowed.
     * @param strategy how should the {@code Observable} react to buffer overflows.
     * @return this builder (allowing for further chained calls)
     * @see <a href="http://reactivex.io/documentation/operators/backpressure.html">ReactiveX operators documentation: backpressure operators</a>
     */
    @NotNull
    public MesosClientBuilder<Send, Receive> onBackpressureBuffer(
            final long capacity,
            @Nullable final Action0 onOverflow,
            @NotNull final BackpressureOverflow.Strategy strategy
            ) {
        this.backpressureTransformer = observable -> observable.onBackpressureBuffer(capacity, onOverflow, strategy);
        return this;
    }

    /**
     * Builds the instance of {@link MesosClient} that has been configured by this builder.
     * All items are expected to have non-null values, if any item is null an exception will be thrown.
     * @return The configured {@link MesosClient}
     */
    @NotNull
    public final MesosClient<Send, Receive> build() {
        return new MesosClient<>(
            checkNotNull(mesosUri),
            checkNotNull(applicationUserAgentEntry),
            checkNotNull(sendCodec),
            checkNotNull(receiveCodec),
            checkNotNull(subscribe),
            checkNotNull(streamProcessor),
            checkNotNull(backpressureTransformer),
            headerSupplier
        );
    }

}
