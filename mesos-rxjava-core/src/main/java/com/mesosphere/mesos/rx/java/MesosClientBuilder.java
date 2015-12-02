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
import rx.Observable;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder used to create a {@link MesosClient}.
 * <p>
 * PLEASE NOTE: All methods in this class function as "set" rather than "copy with new value"
 * @param <Send>       The type of objects that will be sent to Mesos
 * @param <Receive>    The type of objects are expected from Mesos
 */
public final class MesosClientBuilder<Send, Receive> {

    private URI mesosUri;
    private Function<Class<?>, UserAgentEntry> applicationUserAgentEntry;
    private MessageCodec<Send> sendCodec;
    private MessageCodec<Receive> receiveCodec;
    private Send subscribe;
    private Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor;

    private MesosClientBuilder() {}

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
     * Sets the function used to create a {@link UserAgentEntry} to be included the {@code User-Agent} header
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
     * @param subscribe     The {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#SUBSCRIBE}
     *                      {@link org.apache.mesos.v1.scheduler.Protos.Call} to be sent to Mesos
     *                      when opening the event stream.
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
     * The stream the users will source from is an {@link Observable} of {@code Receive}s. With this stream
     * source a number of functions can be applied to transform/interact/evaluate the stream.
     * <p>
     * The output of this function represents the users reaction to each event represented as an
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
            checkNotNull(streamProcessor)
        );
    }

}
