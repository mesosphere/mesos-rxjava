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

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.AbstractHttpContentHolder;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.apache.mesos.rx.java.recordio.RecordIOOperator;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public final class MesosSchedulerClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MesosSchedulerClient.class);

    @NotNull
    private final String contentType;
    @NotNull
    private final String accept;

    @NotNull
    private final HttpClient<ByteBuf, ByteBuf> httpClient;

    public MesosSchedulerClient() {
        this("localhost", 5050);
    }

    public MesosSchedulerClient(@NotNull final String host, final int port) {
        this("rx-mesos", "application/x-protobuf", "application/x-protobuf", host, port);
    }

    public MesosSchedulerClient(
        @NotNull final String name,
        @NotNull final String contentType,
        @NotNull final String accept,
        @NotNull final String host,
        final int port
    ) {
        this.contentType = contentType;
        this.accept = accept;

        httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(host, port)
            .withName(name)
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();
    }

    @NotNull
    public Observable<Protos.Event> openEventStream(@NotNull final Call subscription) {
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
            .map((byte[] bs) -> {
                try {
                    return Protos.Event.parseFrom(bs);
                } catch (InvalidProtocolBufferException e) {
                    return ProtoUtils.errorMessage("Ruh-Ro: " + e.getMessage());
                }
            })
            .doOnNext(event -> LOGGER.trace("Observed Event: {}", ProtoUtils.protoToString(event)))
            .doOnError(t -> LOGGER.warn("doOnError", t))
            ;
    }

    @NotNull
    public Observable<HttpClientResponse<ByteBuf>> sink(@NotNull final Observable<Call> spout) {
        return Observable.concat(
            spout
                .doOnNext(call -> LOGGER.trace("Sending Call: {}", ProtoUtils.protoToString(call)))
                .map(this::createPost)
                .subscribeOn(Schedulers.io())
                .map(httpClient::submit)
        );
    }

    @NotNull
    private HttpClientRequest<ByteBuf> createPost(final @NotNull Call call) {
        final byte[] bytes = call.toByteArray();
        return HttpClientRequest.createPost("/api/v1/scheduler")
            .withHeader("Content-Type", contentType)
            .withHeader("Accept", accept)
            .withContent(bytes);
    }


}
