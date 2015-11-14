/*
 *    Copyright (C) 2015 Apache Software Foundation (ASF)
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

package org.apache.mesos.rx.java;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import org.apache.mesos.rx.java.recordio.RecordIOOperator;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static rx.Observable.just;

public final class MesosSchedulerClient<Send, Receive> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosSchedulerClient.class);

    @NotNull
    public static MesosSchedulerClient<Call, Protos.Event> usingProtos(
        @NotNull final URI mesosUri,
        @NotNull final Function<Class<?>, UserAgentEntry> applicationUserAgentEntry
    ) {
        return new MesosSchedulerClient<>(
            mesosUri,
            applicationUserAgentEntry,
            MessageCodecs.PROTOS_CALL,
            MessageCodecs.PROTOS_EVENT
        );
    }


    @NotNull
    private final MessageCodec<Send> sendCodec;

    @NotNull
    private final MessageCodec<Receive> receiveCodec;

    @NotNull
    private final HttpClient<ByteBuf, ByteBuf> httpClient;

    @NotNull
    @VisibleForTesting
    final Func1<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;


    public MesosSchedulerClient(
        @NotNull final URI mesosUri,
        @NotNull final Function<Class<?>, UserAgentEntry> applicationUserAgentEntry,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec
    ) {
        this.sendCodec = sendCodec;
        this.receiveCodec = receiveCodec;

        final UserAgent userAgent = new UserAgent(
            applicationUserAgentEntry,
            UserAgentEntries.userAgentEntryForMavenArtifact("org.apache.mesos.rx.java", "mesos-rxjava-core"),
            UserAgentEntries.userAgentEntryForGradleArtifact("rxnetty")
        );

        httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(mesosUri.getHost(), mesosUri.getPort())
            .withName(userAgent.getEntries().get(0).getName())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        createPost = (Send s) -> {
            final byte[] bytes = sendCodec.encode(s);
            final HttpClientRequest<ByteBuf> request =
                HttpClientRequest.createPost(mesosUri.getPath())
                    .withHeader("Content-Type", sendCodec.mediaType())
                    .withHeader("Accept", receiveCodec.mediaType())
                    .withHeader("User-Agent", userAgent.toString());

            return just(
                request
                    .withContent(bytes)
            );
        };
    }

    @NotNull
    public Observable<Receive> openEventStream(@NotNull final Send subscription) {
        return createPost.call(subscription)
            .flatMap(httpClient::submit)
            .subscribeOn(Schedulers.io())
            .flatMap(verifyResponseOk(subscription))
            .lift(new RecordIOOperator())
            .observeOn(Schedulers.computation()) // TODO: Figure out how to move this before the lift
            /* Begin temporary back-pressure */
            .buffer(250, TimeUnit.MILLISECONDS)
            .flatMap(Observable::from)
            /* end temporary back-pressure */
            .map(receiveCodec::decode)
            .doOnNext(event -> LOGGER.trace("Observed Event: {}", receiveCodec.show(event)))
            .doOnError(t -> LOGGER.warn("doOnError", t))
            ;
    }

    @NotNull
    public Subscription sink(@NotNull final Observable<SinkOperation<Send>> spout) {
        final Subscriber<SinkOperation<Send>> subscriber = new SinkSubscriber<>(httpClient, createPost);
        return spout
            .subscribeOn(Rx.compute())
            .observeOn(Rx.compute())
            .subscribe(subscriber);
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

}
