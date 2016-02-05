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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgent;
import com.mesosphere.mesos.rx.java.util.UserAgentEntry;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import com.mesosphere.mesos.rx.java.recordio.RecordIOOperator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.Exceptions;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;

import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForGradleArtifact;
import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForMavenArtifact;
import static rx.Observable.just;

/**
 * This class performs the necessary work to create an {@link Observable} of {@code Receive} from Mesos'
 * <a target="_blank" href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md">HTTP Scheduler API</a>
 * @param <Send>       The type of Objects to be sent to Mesos
 * @param <Receive>    The type of Objects to expect from Mesos
 * @see MesosClientBuilder
 * @see MesosClientBuilders
 */
public final class MesosClient<Send, Receive> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosClient.class);

    @NotNull
    private final ExecutorService exec = Executors.newSingleThreadExecutor(r -> new Thread(r, "stream-monitor-thread"));

    @NotNull
    private final MessageCodec<Send> sendCodec;
    @NotNull
    private final MessageCodec<Receive> receiveCodec;
    @NotNull
    private final HttpClient<ByteBuf, ByteBuf> httpClient;
    @NotNull
    private final Send subscribe;
    @NotNull
    private final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor;

    @NotNull
    @VisibleForTesting
    final Func1<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;

    MesosClient(
        @NotNull final URI mesosUri,
        @NotNull final Function<Class<?>, UserAgentEntry> applicationUserAgentEntry,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec,
        @NotNull final Send subscribe,
        @NotNull final Function<Observable<Receive>, Observable<Optional<SinkOperation<Send>>>> streamProcessor
    ) {
        this.sendCodec = sendCodec;
        this.receiveCodec = receiveCodec;
        this.subscribe = subscribe;
        this.streamProcessor = streamProcessor;

        final UserAgent userAgent = new UserAgent(
            applicationUserAgentEntry,
            userAgentEntryForMavenArtifact("com.mesosphere.mesos.rx.java", "mesos-rxjava-core"),
            userAgentEntryForGradleArtifact("rxnetty")
        );

        httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(mesosUri.getHost(), mesosUri.getPort())
            .withName(userAgent.getEntries().get(0).getName())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        createPost = curryCreatePost(mesosUri, sendCodec, receiveCodec, userAgent);
    }

    @NotNull
    public AwaitableSubscription openStream() {
        final Observable<Receive> receives = createPost.call(subscribe)
            .flatMap(httpClient::submit)
            .subscribeOn(Schedulers.io())
            .flatMap(verifyResponseOk(subscribe))
            .lift(new RecordIOOperator())
            .observeOn(Schedulers.computation()) // TODO: Figure out how to move this before the lift
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

    @NotNull
    private Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>> verifyResponseOk(final @NotNull Send subscription) {
        return resp -> {
            final HttpResponseStatus status = resp.getStatus();
            final int code = status.code();

            if (code == 200) {
                return resp.getContent();
            } else {
                //TODO: Figure out how to get these subscribe exceptions to propagate up so that the observable dies
                final HttpResponseHeaders headers = resp.getHeaders();
                final List<Map.Entry<String, String>> entries = headers.entries();
                String errorMessage;
                if (headers.getContentLength() > 0) {
                    errorMessage = resp.getContent()
                        .map(r -> r.toString(CharsetUtil.UTF_8))
                        .toBlocking()
                        .first();
                } else {
                    errorMessage = "";
                }

                final MesosClientErrorContext context = new MesosClientErrorContext(code, errorMessage, entries);
                if (400 <= code && code < 500) {
                    throw new MesosClientException(subscription, context);
                } else if (500 <= code && code < 600) {
                    throw new MesosServerException(subscription, context);
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

    /**
     * Turns an Observable into an AwaitableSubscription
     */
    private static final class ObservableAwaitableSubscription implements AwaitableSubscription {
        @NotNull
        private final Observable<Optional<Throwable>> observable;
        @NotNull
        private final Subscription subscription;

        public ObservableAwaitableSubscription(
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
    @VisibleForTesting
    static final class SubscriberDecorator<T> extends Subscriber<T> implements Callable<Optional<Throwable>> {
        @NotNull
        private final Subscriber<T> delegate;

        @NotNull
        private final BlockingQueue<Optional<Throwable>> queue;

        public SubscriberDecorator(@NotNull final Subscriber<T> delegate) {
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
    @VisibleForTesting
    static <Send, Receive> Func1<Send, Observable<HttpClientRequest<ByteBuf>>> curryCreatePost(
        @NotNull final URI mesosUri,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec,
        @NotNull final UserAgent userAgent
    ) {
        return (Send s) -> {
            final byte[] bytes = sendCodec.encode(s);
            HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(mesosUri.getPath())
                .withHeader("User-Agent", userAgent.toString())
                .withHeader("Content-Type", sendCodec.mediaType())
                .withHeader("Accept", receiveCodec.mediaType());

            final String userInfo = mesosUri.getUserInfo();
            if (userInfo != null) {
                //Won't actually work until https://issues.apache.org/jira/browse/MESOS-3923 is fixed
                request = request.withHeader(
                    HttpHeaders.AUTHORIZATION,
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
