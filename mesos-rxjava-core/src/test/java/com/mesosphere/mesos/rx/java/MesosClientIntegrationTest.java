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

import com.mesosphere.mesos.rx.java.util.UserAgentEntries;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mesosphere.mesos.rx.java.util.SchedulerCalls.decline;
import static com.mesosphere.mesos.rx.java.util.SchedulerCalls.subscribe;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public final class MesosClientIntegrationTest {

    @Rule
    public Timeout timeoutRule = new Timeout(5_000, TimeUnit.MILLISECONDS);

    @Test
    public void testStreamDoesNotRunWhenSubscribeFails_mesos4xxResponse() throws Throwable {
        final RequestHandler<ByteBuf, ByteBuf> handler = (request, response) -> {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return response.close();
        };
        final HttpServer<ByteBuf, ByteBuf> server = RxNetty.createHttpServer(0, handler);
        server.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", server.getServerPort()));
        final String fwId = "test-" + UUID.randomUUID();
        final MesosClient<Protos.Call, Protos.Event> client = createClient(uri, fwId);

        try {
            client.openStream().await();
        } catch (Mesos4xxException e) {
            // expected
            final MesosClientErrorContext ctx = e.getContext();
            assertThat(ctx.getStatusCode()).isEqualTo(400);
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testStreamDoesNotRunWhenSubscribeFails_mesos5xxResponse() throws Throwable {
        final RequestHandler<ByteBuf, ByteBuf> handler = (request, response) -> {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return response.close();
        };
        final HttpServer<ByteBuf, ByteBuf> server = RxNetty.createHttpServer(0, handler);
        server.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", server.getServerPort()));
        final String fwId = "test-" + UUID.randomUUID();
        final MesosClient<Protos.Call, Protos.Event> client = createClient(uri, fwId);

        try {
            client.openStream().await();
        } catch (Mesos5xxException e) {
            // expected
            final MesosClientErrorContext ctx = e.getContext();
            assertThat(ctx.getStatusCode()).isEqualTo(500);
        } finally {
            server.shutdown();
        }
    }

    @NotNull
    private static MesosClient<Protos.Call, Protos.Event> createClient(final URI uri, final String fwId) {
        return MesosClientBuilders.schedulerUsingProtos()
            .mesosUri(uri)
            .applicationUserAgentEntry(UserAgentEntries.literal("test", "test"))
            .processStream(events ->
                events
                    .doOnNext(e -> fail("event stream should never start"))
                    .filter(e -> e.getType() == Protos.Event.Type.OFFERS)
                    .map(e ->
                        decline(
                            FrameworkID.newBuilder().setValue(fwId).build(),
                            e.getOffers().getOffersList().stream()
                                .map(org.apache.mesos.v1.Protos.Offer::getId)
                                .collect(Collectors.toList())
                        )
                    )
                    .map(SinkOperations::create)
                    .map(Optional::of))
            .subscribe(subscribe(fwId, "root", "test", 15))
            .build();
    }

}
