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

package com.mesosphere.mesos.rx.java;

import com.mesosphere.mesos.rx.java.test.StringMessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgentEntries;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.exceptions.MissingBackpressureException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class MesosClientBackpressureIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MesosClientBackpressureIntegrationTest.class);

    static int msgNo = 0;

    @Rule
    public Timeout timeoutRule = new Timeout(10_000, TimeUnit.MILLISECONDS);

    @Test
    @Ignore
    public void testBurstyObservable_missingBackpressureException() throws Throwable {
        final String subscribedMessage = "{\"type\": \"SUBSCRIBED\",\"subscribed\": {\"framework_id\": {\"value\":\"12220-3440-12532-2345\"},\"heartbeat_interval_seconds\":15.0}";
        final String heartbeatMessage = "{\"type\":\"HEARTBEAT\"}";
        final byte[] hmsg = heartbeatMessage.getBytes(StandardCharsets.UTF_8);
        final byte[] hbytes = String.format("%d\n", heartbeatMessage.getBytes().length).getBytes(StandardCharsets.UTF_8);

        final RequestHandler<ByteBuf, ByteBuf> handler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            response.getHeaders().setHeader("Content-Type", "text/plain;charset=utf-8");
            writeRecordIOMessage(response, subscribedMessage);
            for (int i = 0; i < 20000; i++) {
                response.writeBytes(hbytes);
                response.writeBytes(hmsg);
            }
            return response.flush();
        };
        final HttpServer<ByteBuf, ByteBuf> server = RxNetty.createHttpServer(0, handler);
        server.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", server.getServerPort()));
        final MesosClient<String, String> client = createClientForStreaming(uri).build();

        try {
            client.openStream().await();
            fail("Expect an exception to be propagated up due to backpressure");
        } catch (MissingBackpressureException e) {
            // expected
            e.printStackTrace();
            assertThat(e.getMessage()).isNullOrEmpty();
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testBurstyObservable_unboundedBufferSucceeds() throws Throwable {
        msgNo = 0;
        final int numMessages = 20000;
        final String subscribedMessage = "{\"type\": \"SUBSCRIBED\",\"subscribed\": {\"framework_id\": {\"value\":\"12220-3440-12532-2345\"},\"heartbeat_interval_seconds\":15.0}";
        final String heartbeatMessage = "{\"type\":\"HEARTBEAT\"}";
        final RequestHandler<ByteBuf, ByteBuf> handler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            response.getHeaders().setHeader("Content-Type", "text/plain;charset=utf-8");
            writeRecordIOMessage(response, subscribedMessage);
            for (int i = 0; i < numMessages; i++) {
                writeRecordIOMessage(response, heartbeatMessage);
            }
            return response.close();
        };
        final HttpServer<ByteBuf, ByteBuf> server = RxNetty.createHttpServer(0, handler);
        server.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", server.getServerPort()));
        final MesosClient<String, String> client = createClientForStreaming(uri)
                .onBackpressureBuffer()
                .build();

        try {
            client.openStream().await();
        } finally {
            // 20000 heartbeats PLUS 1 subscribe
            assertEquals("All heartbeats received (plus the subscribed)", 1 + numMessages, msgNo);
            server.shutdown();
        }
    }

    private void writeRecordIOMessage(HttpServerResponse<ByteBuf> response, String msg) {
        response.writeBytesAndFlush(String.format("%d\n", msg.getBytes().length).getBytes(StandardCharsets.UTF_8));
        response.writeBytesAndFlush(msg.getBytes(StandardCharsets.UTF_8));
    }

    private static MesosClientBuilder<String, String> createClientForStreaming(final URI uri) {
        return MesosClientBuilder.<String, String>newBuilder()
                .sendCodec(StringMessageCodec.UTF8_STRING)
                .receiveCodec(StringMessageCodec.UTF8_STRING)
                .mesosUri(uri)
                .applicationUserAgentEntry(UserAgentEntries.literal("test", "test"))
                .processStream(events ->
                        events
                                .doOnNext(e -> LOGGER.debug(++msgNo + " : " + e))
                                .map(e -> Optional.empty()))
                .subscribe("subscribe");
    }

}
