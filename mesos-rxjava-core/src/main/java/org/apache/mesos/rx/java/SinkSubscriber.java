package org.apache.mesos.rx.java;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.functions.Func1;

import java.util.List;
import java.util.Map;

final class SinkSubscriber<Send> extends Subscriber<SinkOperation<Send>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkSubscriber.class);

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
                .subscribeOn(Rx.compute())
                .subscribe(resp -> {
                    final HttpResponseStatus status = resp.getStatus();
                    final int code = status.code();

                    if (code == 202) {
                        /* This is success */
                        op.onCompleted();  // TODO: Try and make sure this is actually executed on the compute thread
                    } else {
                        resp.getContent()
                            .map(buf -> {
                                final String errorMessage = buf.toString(CharsetUtil.UTF_8);
                                final HttpResponseHeaders headers = resp.getHeaders();
                                final List<Map.Entry<String, String>> entries = headers.entries();
                                return new MesosClientErrorContext(code, errorMessage, entries);
                            })
                            // TODO: Schedule on computation()
                            .observeOn(Rx.compute())
                            .forEach(context -> {
                                if (400 <= code && code < 500) {
                                    // client error
                                    op.onError(new MesosClientException(toSink, context));
                                } else if (500 <= code && code < 600) {
                                    // client error
                                    op.onError(new MesosServerException(toSink, context));
                                } else {
                                    // Unknown error
                                    LOGGER.warn("Unhandled error: context = {}", context);
                                }
                            });
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
