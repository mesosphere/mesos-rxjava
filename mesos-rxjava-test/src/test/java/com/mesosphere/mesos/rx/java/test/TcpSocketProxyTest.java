/*
 *    Copyright (C) 2016 Mesosphere, Inc
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

package com.mesosphere.mesos.rx.java.test;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.AbstractHttpContentHolder;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.server.HttpServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class TcpSocketProxyTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpSocketProxyTest.class);

    @Rule
    public Timeout timeout = new Timeout(2, TimeUnit.SECONDS);

    private HttpServer<ByteBuf, ByteBuf> server;

    @Before
    public void setUp() throws Exception {
        server = RxNetty.createHttpServer(0, (request, response) -> {
            response.writeString("Hello World");
            return response.close();
        });
        server.start();
    }

    @Test
    public void testConnectionTerminatedOnClose() throws Exception {
        final TcpSocketProxy proxy = new TcpSocketProxy(
            new InetSocketAddress("localhost", 0),
            new InetSocketAddress("localhost", server.getServerPort())
        );
        proxy.start();

        final int listenPort = proxy.getListenPort();
        final HttpClient<ByteBuf, ByteBuf> client = RxNetty.createHttpClient("localhost", listenPort);

        final String first = client.submit(HttpClientRequest.createGet("/"))
            .flatMap(AbstractHttpContentHolder::getContent)
            .map(bb -> bb.toString(StandardCharsets.UTF_8))
            .toBlocking()
            .first();

        assertThat(first).isEqualTo("Hello World");
        LOGGER.info("first request done");
        proxy.shutdown();
        if (proxy.isShutdown()) {
            proxy.close();
        } else {
            fail("proxy should have been shutdown");
        }

        try {
            final URI uri = URI.create(String.format("http://localhost:%d/", listenPort));
            uri.toURL().getContent();
            fail("Shouldn't have been able to get content");
        } catch (IOException e) {
            // expected
        }
    }
}
