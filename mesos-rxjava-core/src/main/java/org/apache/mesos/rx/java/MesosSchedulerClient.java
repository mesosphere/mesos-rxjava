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

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.AbstractHttpContentHolder;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.apache.mesos.rx.java.recordio.RecordIOOperator;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public final class MesosSchedulerClient<Send, Receive> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosSchedulerClient.class);

    @NotNull
    public static MesosSchedulerClient<Protos.Call, Protos.Event> usingProtos(
        @NotNull final String host,
        final int port
    ) {
        return new MesosSchedulerClient<>(host, port, "rx-mesos", MessageCodecs.PROTOS_CALL, MessageCodecs.PROTOS_EVENT);
    }


    @NotNull
    private final MessageCodec<Send> sendCodec;

    @NotNull
    private final MessageCodec<Receive> receiveCodec;

    @NotNull
    private final HttpClient<ByteBuf, ByteBuf> httpClient;


    public MesosSchedulerClient(
        @NotNull final String host,
        final int port,
        @NotNull final String name,
        @NotNull final MessageCodec<Send> sendCodec,
        @NotNull final MessageCodec<Receive> receiveCodec
    ) {
        this.sendCodec = sendCodec;
        this.receiveCodec = receiveCodec;

        httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(host, port)
            .withName(name)
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();
    }

    @NotNull
    public Observable<Receive> openEventStream(@NotNull final Send subscription) {
        final HttpClientRequest<ByteBuf> registerRequest = createPost(subscription);

        return httpClient.submit(registerRequest)
            .subscribeOn(Schedulers.io())
            .flatMap(AbstractHttpContentHolder::getContent)
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
    public Observable<HttpClientResponse<ByteBuf>> sink(@NotNull final Observable<Send> spout) {
        return Observable.concat(
            spout
                .doOnNext(call -> LOGGER.trace("Sending Call: {}", sendCodec.show(call)))
                .map(this::createPost)
                .subscribeOn(Schedulers.io())
                .map(httpClient::submit)
        );
    }

    @NotNull
    private HttpClientRequest<ByteBuf> createPost(@NotNull final Send call) {
        final byte[] bytes = sendCodec.encode(call);
        return HttpClientRequest.createPost("/api/v1/scheduler")
            .withHeader("Content-Type", sendCodec.mediaType())
            .withHeader("Accept", receiveCodec.mediaType())
            .withContent(bytes);
    }


}
