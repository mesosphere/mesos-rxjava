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

package com.mesosphere.mesos.rx.java.test.simulation;

import com.mesosphere.mesos.rx.java.test.StringMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.subjects.BehaviorSubject;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public final class MesosSchedulerSimulationTest {

    private final MesosSchedulerSimulation<String, String> mesosSchedulerSimulation = new MesosSchedulerSimulation<>(
        BehaviorSubject.create(),
        StringMessageCodec.UTF8_STRING,
        StringMessageCodec.UTF8_STRING,
        "subscribe"::equals
    );
    private int serverPort;

    @Before
    public void setUp() throws Exception {
        serverPort = mesosSchedulerSimulation.start();
    }

    @After
    public void tearDown() throws Exception {
        mesosSchedulerSimulation.shutdown();
    }

    @Test
    public void server202_forValidCall() throws Exception {
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClientResponse<ByteBuf> response = sendCall(uri, "decline");

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.ACCEPTED);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

        assertThat(mesosSchedulerSimulation.getCallsReceived()).hasSize(1);
        assertThat(mesosSchedulerSimulation.getCallsReceived()).contains("decline");
    }

    @Test
    public void server404() throws Exception {
        final URI uri = URI.create(String.format("http://localhost:%d/something", serverPort));
        final HttpClientResponse<ByteBuf> response = sendCall(uri, "decline");

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.NOT_FOUND);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

        assertThat(mesosSchedulerSimulation.getCallsReceived()).hasSize(0);
    }

    @Test
    public void server400_nonPost() throws Exception {
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet(uri.getPath())
            .withHeader("Accept", StringMessageCodec.UTF8_STRING.mediaType());

        final HttpClientResponse<ByteBuf> response = httpClient.submit(request)
            .toBlocking()
            .last();

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

        assertThat(mesosSchedulerSimulation.getCallsReceived()).hasSize(0);
    }

    @Test
    public void server400_invalidContentType() throws Exception {
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final byte[] data = StringMessageCodec.UTF8_STRING.encode("decline");
        final HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", "application/octet-stream")
            .withHeader("Accept", "application/octet-stream")
            .withContent(data);

        final HttpClientResponse<ByteBuf> response = httpClient.submit(request)
            .toBlocking()
            .last();

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

        assertThat(mesosSchedulerSimulation.getCallsReceived()).hasSize(0);
    }

    @Test
    public void server400_emptyBody() throws Exception {
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", StringMessageCodec.UTF8_STRING.mediaType())
            .withHeader("Accept", StringMessageCodec.UTF8_STRING.mediaType())
            .withContent(new byte[]{});

        final HttpClientResponse<ByteBuf> response = httpClient.submit(request)
            .toBlocking()
            .last();

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

        assertThat(mesosSchedulerSimulation.getCallsReceived()).hasSize(0);
    }

    @NotNull
    private static HttpClientResponse<ByteBuf> sendCall(final URI uri, final String call) {
        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final byte[] data = call.getBytes(StandardCharsets.UTF_8);
        final HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", StringMessageCodec.UTF8_STRING.mediaType())
            .withHeader("Accept", StringMessageCodec.UTF8_STRING.mediaType())
            .withContent(data);

        return httpClient.submit(request)
            .toBlocking()
            .last();
    }
}
