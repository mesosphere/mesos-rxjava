/*
 *    Copyright (C) 2017 Mesosphere, Inc
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
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.reactivex.netty.protocol.http.UnicastContentSubject;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import rx.functions.Action1;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ResponseUtilsTest {
    
    @Test
    public void attemptToReadErrorResponse_plainString() throws Exception {
        final String errMsg = "some response";
        final byte[] bytes = errMsg.getBytes(StandardCharsets.UTF_8);
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(bytes), (headers) -> {
            headers.add("Content-Type", "text/plain;charset=utf-8");
            headers.add("Content-Length", bytes.length);
        });

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isEqualTo(errMsg);
    }

    @Test
    public void attemptToReadErrorResponse_noContentLength() throws Exception {
        final String errMsg = "some response";
        final byte[] bytes = errMsg.getBytes(StandardCharsets.UTF_8);
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(bytes), (headers) ->
            headers.add("Content-Type", "text/plain;charset=utf-8")
        );

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isEqualTo("");
    }

    @Test
    public void attemptToReadErrorResponse_noContentType() throws Exception {
        final String errMsg = "some response";
        final byte[] bytes = errMsg.getBytes(StandardCharsets.UTF_8);
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(bytes), (headers) ->
            headers.add("Content-Length", bytes.length)
        );

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isEqualTo("Not attempting to decode error response with unspecified Content-Type");
    }

    @Test
    public void attemptToReadErrorResponse_contentLengthIsZero() throws Exception {
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(new byte[]{}), (headers) ->
            headers.add("Content-Length", "0")
        );

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isEqualTo("");
    }

    @Test
    public void attemptToReadErrorResponse_nonTextPlain() throws Exception {
        final String errMsg = "{\"some\":\"json\"}";
        final byte[] bytes = errMsg.getBytes(StandardCharsets.UTF_8);
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(bytes), (headers) -> {
            headers.add("Content-Type", "application/json;charset=utf-8");
            headers.add("Content-Length", bytes.length);
        });

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isEqualTo("Not attempting to decode error response of type 'application/json;charset=utf-8' as string");
    }

    @Test
    public void attemptToReadErrorResponse_responseContentIgnoredByDefaultWhenNotString() throws Exception {
        final String errMsg = "lies";
        final byte[] bytes = errMsg.getBytes(StandardCharsets.UTF_8);
        final HttpClientResponse<ByteBuf> resp = response(Unpooled.copiedBuffer(bytes), (headers) -> {
            headers.add("Content-Type", "application/json;charset=utf-8");
            headers.add("Content-Length", bytes.length);
        });

        final String err = ResponseUtils.attemptToReadErrorResponse(resp).toBlocking().first();
        assertThat(err).isNotEqualTo("lies");

        try {
            resp.getContent().toBlocking().first();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("Content stream is already disposed.");
        }
    }

    private static HttpClientResponse<ByteBuf> response(
        @NotNull final ByteBuf content,
        @NotNull final Action1<HttpHeaders> headerTransformer
    ) {
        final DefaultHttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        headerTransformer.call(nettyResponse.headers());
        final UnicastContentSubject<ByteBuf> subject = UnicastContentSubject.create(1000, TimeUnit.MILLISECONDS);
        subject.onNext(content);
        return new HttpClientResponse<>(
            nettyResponse,
            subject
        );
    }

}
