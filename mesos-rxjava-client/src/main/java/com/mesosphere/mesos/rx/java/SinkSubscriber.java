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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.jetbrains.annotations.NotNull;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.functions.Func1;

import java.util.List;
import java.util.Map;
import java.util.Optional;

final class SinkSubscriber<Send> extends Subscriber<SinkOperation<Send>> {

    @NotNull
    private final HttpClient<ByteBuf, ByteBuf> httpClient;
    @NotNull
    private final Func1<Send, Observable<HttpClientRequest<ByteBuf>>> createPost;

    SinkSubscriber(
        @NotNull final HttpClient<ByteBuf, ByteBuf> httpClient,
        @NotNull final Func1<Send, Observable<HttpClientRequest<ByteBuf>>> createPost
    ) {
        this.httpClient = httpClient;
        this.createPost = createPost;
    }

    @Override
    public void onNext(final SinkOperation<Send> op) {
        try {
            final Send toSink = op.getThingToSink();
            createPost.call(toSink)
                .flatMap(httpClient::submit)
                .flatMap(resp -> {
                    final HttpResponseStatus status = resp.getStatus();
                    final int code = status.code();

                    if (code == 202) {
                        /* This is success */
                        return Observable.just(Optional.<MesosException>empty());
                    } else {
                        final HttpResponseHeaders headers = resp.getHeaders();
                        return ResponseUtils.attemptToReadErrorResponse(resp)
                            .map(msg -> {
                                final List<Map.Entry<String, String>> entries = headers.entries();
                                final MesosClientErrorContext context = new MesosClientErrorContext(code, msg, entries);
                                MesosException error;
                                if (400 <= code && code < 500) {
                                    // client error
                                    error = new Mesos4xxException(toSink, context);
                                } else if (500 <= code && code < 600) {
                                    // client error
                                    error = new Mesos5xxException(toSink, context);
                                } else {
                                    // something else that isn't success but not an error as far as http is concerned
                                    error = new MesosException(toSink, context);
                                }
                                return Optional.of(error);
                            });
                    }
                })
                .observeOn(Rx.compute())
                .subscribe(exception -> {
                    if (!exception.isPresent()) {
                        op.onCompleted();
                    } else {
                        op.onError(exception.get());
                    }
                });
        } catch (Throwable e) {
            Exceptions.throwIfFatal(e);
            op.onError(e);
        }
    }

    @Override
    public void onError(final Throwable e) {
        Exceptions.throwIfFatal(e);
    }

    @Override
    public void onCompleted() {
    }
}
