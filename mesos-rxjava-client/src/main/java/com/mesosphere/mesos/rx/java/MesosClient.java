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

import com.mesosphere.mesos.rx.java.recordio.RecordIOOperator;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgent;
import com.mesosphere.mesos.rx.java.util.UserAgentEntry;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.Exceptions;
import rx.functions.Func1;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForGradleArtifact;
import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForMavenArtifact;
import static rx.Observable.just;

/**
 * This class performs the necessary work to create an {@link Observable} of {@code Receive} from Mesos'
 * HTTP APIs, one of which is the
 * <a target="_blank" href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md">HTTP Scheduler API</a>
 * @param <Send>       The type of Objects to be sent to Mesos
 * @param <Receive>    The type of Objects to expect from Mesos
 * @see MesosClientBuilder
 */
public final class MesosClient<Send, Receive> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosClient.class);

    private static final String MESOS_STREAM_ID = "Mesos-Stream-Id";

    @NotNull
    private final ExecutorService exec = Executors.newSingleThreadExecutor(r -> new Thread(r, "stream-monitor-thread"));

    @NotNull
    private final URI mesosUri;
    @NotNull
    private final MessageCodec<Receive> receiveCodec;
    @NotNull
    private final Send subscribe;
    @NotNull
    private final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor;

    @NotNull
    // @VisibleForTesting
    final Func1<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;
    @NotNull
    private final UserAgent userAgent;

    @NotNull
    private final AtomicReference<String> mesosStreamId = new AtomicReference<>(null);

    MesosClient(
        @NotNull final URI mesosUri,
        @NotNull final Function<Class<?>, UserAgentEntry> applicationUserAgentEntry,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec,
        @NotNull final Send subscribe,
        @NotNull final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor
    ) {
        this.mesosUri = mesosUri;
        this.receiveCodec = receiveCodec;
        this.subscribe = subscribe;
        this.streamProcessor = streamProcessor;

        userAgent = new UserAgent(
            applicationUserAgentEntry,
            userAgentEntryForMavenArtifact("com.mesosphere.mesos.rx.java", "mesos-rxjava-client"),
            userAgentEntryForGradleArtifact("rxnetty")
        );

        createPost = curryCreatePost(mesosUri, sendCodec, receiveCodec, userAgent, mesosStreamId);
    }

    /**
     * Sends the subscribe call to Mesos and starts processing the stream of {@code Receive} events.
     * The {@code streamProcessor} function provided to the constructor will be applied to the stream of events
     * received from Mesos.
     * <p>
     * The stream processing will then process any {@link SinkOperation} that should be sent to Mesos.
     * @return The subscription representing the processing of the event stream. This subscription can then be used
     * to block the invoking thread using {@link AwaitableSubscription#await()} (For example to block a main thread
     * from exiting while events are being processed.)
     */
    @NotNull
    public AwaitableSubscription openStream() {

        final URI uri = resolveMesosUri(mesosUri);

        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), getPort(uri))
            .withName(userAgent.getEntries().get(0).getName())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final Observable<Receive> receives = createPost.call(subscribe)
            .flatMap(httpClient::submit)
            .subscribeOn(Rx.io())
            .flatMap(verifyResponseOk(subscribe, mesosStreamId, receiveCodec.mediaType()))
            .lift(new RecordIOOperator())
            .observeOn(Rx.compute())
            /* Begin temporary back-pressure */
            .buffer(250, TimeUnit.MILLISECONDS)
            .flatMap(Observable::from)
            /* end temporary back-pressure */
            .map(receiveCodec::decode)
            ;

        final Observable<SinkOperation<Send>> sends = streamProcessor.apply(receives)
            .filter(Optional::isPresent)
            .map(Optional::get);

        final Subscriber<SinkOperation<Send>> subscriber = new SinkSubscriber<>(httpClient, createPost);
        final SubscriberDecorator<SinkOperation<Send>> decorator = new SubscriberDecorator<>(subscriber);
        final Subscription subscription = sends
            .subscribeOn(Rx.compute())
            .observeOn(Rx.compute())
            .subscribe(decorator);

        return new ObservableAwaitableSubscription(Observable.from(exec.submit(decorator)), subscription);
    }

    /**
     * The Mesos HTTP Scheduler API will send a redirect to a client if it is not the leader. The client that is
     * constructed during {@link #openStream} is bound to a specific host and port, due to this behavior
     * we "probe" Mesos to try and find out where it's "master" is before we configure the client.
     *
     * This method will attempt to send a simple GET to {@code mesosUri}, however instead of going to the path
     * specified, it will go to {@code /redirect} and construct a uri relative to mesosUri using the host and port
     * returned in the Location header of the response.
     */
    @NotNull
    private static URI resolveMesosUri(final @NotNull URI mesosUri) {
        final String redirectUri = createRedirectUri(mesosUri);
        LOGGER.info("Probing Mesos server at {}", redirectUri);

        // When sending an individual request (rather than creating a client) RxNetty WILL follow redirects,
        // so here we tell it not to do that for this request by creating the config object below.
        final HttpClient.HttpClientConfig config =
            HttpClient.HttpClientConfig.Builder.fromDefaultConfig()
                .setFollowRedirect(false)
                .build();
        final HttpClientResponse<ByteBuf> redirectResponse =
            RxNetty.createHttpRequest(HttpClientRequest.createGet(redirectUri), config)
                .toBlocking()
                .first();

        return getUriFromRedirectResponse(mesosUri, redirectResponse);
    }

    @NotNull
    // @VisibleForTesting
    static URI getUriFromRedirectResponse(final @NotNull URI mesosUri, @NotNull final HttpClientResponse<ByteBuf> redirectResponse) {
        if (redirectResponse.getStatus().equals(HttpResponseStatus.TEMPORARY_REDIRECT)) {
            final String location = redirectResponse.getHeaders().get("Location");
            final URI newUri = resolveRelativeUri(mesosUri, location);
            LOGGER.info("Using new Mesos URI: {}", newUri);
            return newUri;
        } else {
            LOGGER.info("Continuing with Mesos URI as-is");
            return mesosUri;
        }
    }

    @NotNull
    // @VisibleForTesting
    static URI resolveRelativeUri(final @NotNull URI mesosUri, final String location) {
        final URI relativeUri = mesosUri.resolve(location);
        try {
            return new URI(
                relativeUri.getScheme(),
                relativeUri.getUserInfo(),
                relativeUri.getHost(),
                relativeUri.getPort(),
                mesosUri.getPath(),
                mesosUri.getQuery(),
                mesosUri.getFragment()
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    // @VisibleForTesting
    static String createRedirectUri(@NotNull final URI uri) {
        return uri.toString().replaceAll("/api/v1/(?:scheduler|executor)", "/redirect");
    }

    @NotNull
    // @VisibleForTesting
    static <Send> Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>> verifyResponseOk(
        @NotNull final Send subscription,
        @NotNull final AtomicReference<String> mesosStreamId,
        @NotNull final String receiveMediaType
    ) {
        return resp -> {
            final HttpResponseStatus status = resp.getStatus();
            final int code = status.code();

            final String contentType = resp.getHeaders().get(HttpHeaders.Names.CONTENT_TYPE);
            if (code == 200 && receiveMediaType.equals(contentType)) {
                if (resp.getHeaders().contains(MESOS_STREAM_ID)) {
                    final String streamId = resp.getHeaders().get(MESOS_STREAM_ID);
                    mesosStreamId.compareAndSet(null, streamId);
                }
                return resp.getContent();
            } else {
                final HttpResponseHeaders headers = resp.getHeaders();
                final List<Map.Entry<String, String>> entries = headers.entries();
                resp.ignoreContent();

                final MesosClientErrorContext context = new MesosClientErrorContext(code, entries);
                if (code == 200) {
                    // this means that even though we got back a 200 it's not the sort of response we were expecting
                    // For example hitting an endpoint that returns an html document instead of a document of type
                    // `receiveMediaType`
                    throw new MesosException(
                        subscription,
                        context.withMessage(
                            String.format(
                                "Response had Content-Type \"%s\" expected \"%s\"",
                                contentType,
                                receiveMediaType
                            )
                        )
                    );
                } else if (400 <= code && code < 500) {
                    throw new Mesos4xxException(subscription, context);
                } else if (500 <= code && code < 600) {
                    throw new Mesos5xxException(subscription, context);
                } else {
                    LOGGER.warn("Unhandled error: context = {}", context);
                    // This shouldn't actually ever happen, but it's here for completeness of the if-else tree
                    // that always has to result in an exception being thrown so the compiler is okay with this
                    // lambda
                    throw new IllegalStateException("Unhandled error");
                }
            }
        };
    }

    // @VisibleForTesting
    static int getPort(@NotNull final URI uri) {
        final int uriPort = uri.getPort();
        if (uriPort > 0) {
            return uriPort;
        } else {
            switch (uri.getScheme()) {
                case "http":
                    return 80;
                case "https":
                    return 443;
                default:
                    throw new IllegalArgumentException("URI Scheme must be http or https");
            }
        }
    }

    /**
     * Turns an Observable into an AwaitableSubscription
     */
    private static final class ObservableAwaitableSubscription implements AwaitableSubscription {
        @NotNull
        private final Observable<Optional<Throwable>> observable;
        @NotNull
        private final Subscription subscription;

        private ObservableAwaitableSubscription(
            @NotNull final Observable<Optional<Throwable>> observable,
            @NotNull final Subscription subscription
        ) {
            this.observable = observable;
            this.subscription = subscription;
        }

        @Override
        public void await() throws Throwable {
            final Optional<Throwable> last = observable.toBlocking().last();
            subscription.unsubscribe();
            if (last.isPresent()) {
                throw last.get();
            }
        }

        @Override
        public void unsubscribe() {
            subscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return subscription.isUnsubscribed();
        }
    }

    /**
     * Decorator for {@link Subscriber} that will keep track of "complete"ness and when complete will put an
     * item onto a blocking queue. {@link Callable#call()} will then block until there is an item on the queue.
     * If the item in the queue contains an exception, that exception will be throw back up to the user.
     */
    // @VisibleForTesting
    static final class SubscriberDecorator<T> extends Subscriber<T> implements Callable<Optional<Throwable>> {
        @NotNull
        private final Subscriber<T> delegate;

        @NotNull
        private final BlockingQueue<Optional<Throwable>> queue;

        SubscriberDecorator(@NotNull final Subscriber<T> delegate) {
            this.delegate = delegate;
            queue = new LinkedBlockingDeque<>();
        }

        /**
         * Returns the first item from the queue that marks the end of the stream.
         * If an exception happens when {@link Subscriber#onError(Throwable)} or {@link Subscriber#onCompleted()} is
         * called for {@code delegate} the exception from that method invocation will be the item returned.
         * @return The item that ended the stream.
         * @throws InterruptedException if waiting on the stream to end is interrupted.
         */
        @Override
        public Optional<Throwable> call() throws InterruptedException {
            return queue.take();
        }

        @Override
        public void onNext(final T t) {
            try {
                delegate.onNext(t);
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void onError(final Throwable e) {
            Exceptions.throwIfFatal(e);
            try {
                delegate.onError(e);
                queue.offer(Optional.of(e));
            } catch (Exception e1) {
                queue.offer(Optional.of(e1));
            }
        }

        @Override
        public void onCompleted() {
            try {
                delegate.onCompleted();
                queue.offer(Optional.empty());
            } catch (Exception e) {
                queue.offer(Optional.of(e));
            }
        }
    }

    @NotNull
    // @VisibleForTesting
    static <Send, Receive> Func1<Send, Observable<HttpClientRequest<ByteBuf>>> curryCreatePost(
        @NotNull final URI mesosUri,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec,
        @NotNull final UserAgent userAgent,
        @NotNull final AtomicReference<String> mesosStreamId
    ) {
        return (Send s) -> {
            final byte[] bytes = sendCodec.encode(s);
            HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(mesosUri.getPath())
                .withHeader("User-Agent", userAgent.toString())
                .withHeader("Content-Type", sendCodec.mediaType())
                .withHeader("Accept", receiveCodec.mediaType());

            final String streamId = mesosStreamId.get();
            if (streamId != null) {
                request = request.withHeader(MESOS_STREAM_ID, streamId);
            }

            final String userInfo = mesosUri.getUserInfo();
            if (userInfo != null) {
                request = request.withHeader(
                    HttpHeaders.Names.AUTHORIZATION,
                    String.format("Basic %s", Base64.getEncoder().encodeToString(userInfo.getBytes()))
                );
            }
            return just(
                request
                    .withContent(bytes)
            );
        };
    }
}
