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

import com.google.common.collect.Maps;
import com.mesosphere.mesos.rx.java.test.StringMessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgent;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.reactivex.netty.protocol.http.UnicastContentSubject;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.client.HttpRequestHeaders;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.literal;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosClientTest {

    @Test
    public void testUserAgentContains_MesosRxJavaCore_RxNetty() throws Exception {
        final String clientName = "unit-tests";
        final MesosClient<String, String> client = MesosClientBuilder.<String, String>newBuilder()
            .sendCodec(StringMessageCodec.UTF8_STRING)
            .receiveCodec(StringMessageCodec.UTF8_STRING)
            .mesosUri(URI.create("http://localhost:12345"))
            .applicationUserAgentEntry(literal(clientName, "latest"))
            .subscribe("subscribe")
            .processStream(events -> events.map(e -> Optional.<SinkOperation<String>>empty()))
            .build();

        final HttpClientRequest<ByteBuf> request = client.createPost
            .call("ACK")
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("User-Agent");
        final String ua = headers.get("User-Agent");
        assertThat(ua).startsWith(String.format("%s/%s", clientName, "latest"));
        assertThat(ua).contains("mesos-rxjava-client/");
    }

    @Test
    public void testRequestUriFromPassedUri() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://localhost:12345/glavin/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            ),
            new AtomicReference<>(null)
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
            .toBlocking()
            .first();

        assertThat(request.getUri()).isEqualTo("/glavin/api/v1/scheduler");
    }

    @Test
    public void testBasicAuthHeaderAddedToRequestWhenUserInfoPresentInUri() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://testuser:testpassword@localhost:12345/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            ),
            new AtomicReference<>(null)
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("Authorization");
        final String authorization = headers.get("Authorization");
        assertThat(authorization).isEqualTo("Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk");

        final String base64UserPass = authorization.substring("Basic ".length());
        final String userPass = new String(Base64.getDecoder().decode(base64UserPass));
        assertThat(userPass).isEqualTo("testuser:testpassword");
    }

    @Test
    public void testMesosStreamIdAddedToRequestWhenNonNull() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://localhost:12345/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            ),
            new AtomicReference<>("streamId")
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("Mesos-Stream-Id");
        assertThat(headers.get("Mesos-Stream-Id")).isEqualTo("streamId");
    }

    @Test
    public void testMesosStreamIdNotPresentWhenNull() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://localhost:12345/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            ),
            new AtomicReference<>(null)
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).doesNotContainKeys("Mesos-Stream-Id");
    }

    @Test
    public void testMesosStreamIdIsSavedForSuccessfulSubscribeCall() throws Exception {
        final AtomicReference<String> mesosStreamId = new AtomicReference<>(null);

        final Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>> f = MesosClient.verifyResponseOk(
            "Subscribe",
            mesosStreamId
        );

        final DefaultHttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        nettyResponse.headers().add("Mesos-Stream-Id", "streamId");
        final HttpClientResponse<ByteBuf> response = new HttpClientResponse<>(
            nettyResponse,
            UnicastContentSubject.create(1000, TimeUnit.MILLISECONDS)
        );

        f.call(response);


        assertThat(mesosStreamId.get()).isEqualTo("streamId");
    }

    @Test
    public void testMesosStreamIdIsNotSavedForUnsuccessfulSubscribeCall() throws Exception {
        final AtomicReference<String> mesosStreamId = new AtomicReference<>(null);

        final Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>> f = MesosClient.verifyResponseOk(
            "Subscribe",
            mesosStreamId
        );

        final DefaultHttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
        nettyResponse.headers().add("Mesos-Stream-Id", "streamId");
        final HttpClientResponse<ByteBuf> response = new HttpClientResponse<>(
            nettyResponse,
            UnicastContentSubject.create(1000, TimeUnit.MILLISECONDS)
        );

        try {
            f.call(response);
        } catch (Mesos4xxException e) {
            // expected
        }

        assertThat(mesosStreamId.get()).isEqualTo(null);
    }

    @Test
    public void testGetPort_returnsSpecifiedPort() throws Exception {
        assertThat(MesosClient.getPort(URI.create("http://glavin:500/path"))).isEqualTo(500);
    }

    @Test
    public void testGetPort_returns80ForHttp() throws Exception {
        assertThat(MesosClient.getPort(URI.create("http://glavin/path"))).isEqualTo(80);
    }

    @Test
    public void testGetPort_returns443ForHttps() throws Exception {
        assertThat(MesosClient.getPort(URI.create("https://glavin/path"))).isEqualTo(443);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPort_throwsExceptionWhenNoPortIsSpecifiedAndSchemeIsNotHttpOrHttps() throws Exception {
        MesosClient.getPort(URI.create("ftp://glavin/path"));
    }

    @NotNull
    private static Map<String, String> headersToMap(@NotNull final HttpRequestHeaders headers) {
        final HashMap<String, String> map = Maps.newHashMap();
        for (Map.Entry<String, String> entry : headers.entries()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }
}
