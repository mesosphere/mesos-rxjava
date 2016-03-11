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

package com.mesosphere.mesos.rx.java.test.simulation;

import com.mesosphere.mesos.rx.java.test.Async;
import com.mesosphere.mesos.rx.java.test.RecordIOUtils;
import com.mesosphere.mesos.rx.java.test.StringMessageCodec;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.Observable;
import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.subjects.BehaviorSubject;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.mesosphere.mesos.rx.java.util.CollectionUtils.deepEquals;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosSchedulerSimulationScenariosTest {

    public static final String SUBSCRIBE = "subscribe";
    public static final String SUBSCRIBED = "subscribed";
    public static final String HEARTBEAT = "heartbeat";
    public static final String OFFER_1 = "offer-1";
    public static final String DECLINE = "decline";
    @Rule
    public Timeout timeoutRule = new Timeout(5000, MILLISECONDS);

    @Rule
    public Async async = new Async();

    @Test
    public void awaitServerCallsReceived() throws Exception {
        final List<String> events = newArrayList(
            SUBSCRIBED,
            HEARTBEAT,
            OFFER_1
        );
        final List<byte[]> expected = events.stream()
            .map(StringMessageCodec.UTF8_STRING::encode)
            .map(RecordIOUtils::createChunk)
            .collect(Collectors.toList());

        final MesosSchedulerSimulation<String, String> sim = new MesosSchedulerSimulation<>(
            Observable.from(events),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            SUBSCRIBE::equals
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> subscribe = createRequest(uri, SUBSCRIBE);

        final Observable<byte[]> observable = httpClient.submit(subscribe)
            .flatMap(response -> {
                assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.OK);
                return response.getContent();
            })
            .map(MesosSchedulerSimulationScenariosTest::bufToBytes)
            ;

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        async.run(() -> {
            sub.awaitEvent(3);

            final HttpClientRequest<ByteBuf> decline = createRequest(uri, DECLINE);
            final HttpClientResponse<ByteBuf> declineResult =
                httpClient.submit(decline)
                    .toBlocking()
                    .first();

            assertThat(declineResult.getStatus()).isEqualTo(HttpResponseStatus.ACCEPTED);
        });

        sim.awaitCall(2);
        assertThat(sim.getCallsReceived()).hasSize(2);
        assertThat(sim.getCallsReceived()).isEqualTo(
            newArrayList(SUBSCRIBE, DECLINE)
        );
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void behaviorSubjectSubscribeHeartbeat() throws Exception {
        final List<String> events = newArrayList(
            SUBSCRIBED,
            HEARTBEAT
        );
        final List<byte[]> expected = events.stream()
            .map(StringMessageCodec.UTF8_STRING::encode)
            .map(RecordIOUtils::createChunk)
            .collect(Collectors.toList());

        /*
         * Use of BehaviorSubject here is important. The reason is that BehaviorSubject will buffer any items passed
         * to it's onNext method even if the Subject has not yet been subscribed to. A PublishSubject will only forward
         * events to it's subscriber without any buffering.
         */
        final BehaviorSubject<String> subject = BehaviorSubject.create();
        final MesosSchedulerSimulation<String, String> sim = new MesosSchedulerSimulation<>(
            Observable.from(events),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            SUBSCRIBE::equals
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> request = createRequest(uri, SUBSCRIBE);

        final Observable<byte[]> observable = openStream(httpClient, request);

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        // SUBSCRIBED
        subject.onNext(events.get(0));

        // HEARTBEAT
        subject.onNext(events.get(1));
        sub.awaitEvent(2);
        final List<byte[]> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).hasSize(2);
        assertThat(onNextEvents.get(0)).isEqualTo(expected.get(0));
        assertThat(onNextEvents.get(1)).isEqualTo(expected.get(1));

        assertThat(sim.getCallsReceived()).hasSize(1);
        assertThat(sim.getCallsReceived()).contains(SUBSCRIBE);
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void observableSubscribeHeartbeat() throws Exception {
        final List<String> events = newArrayList(
            SUBSCRIBED,
            HEARTBEAT
        );
        final List<byte[]> expected = events.stream()
            .map(StringMessageCodec.UTF8_STRING::encode)
            .map(RecordIOUtils::createChunk)
            .collect(Collectors.toList());

        final MesosSchedulerSimulation<String, String> sim = new MesosSchedulerSimulation<>(
            Observable.from(events),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            SUBSCRIBE::equals
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> request = createRequest(uri, SUBSCRIBE);

        final Observable<byte[]> observable = openStream(httpClient, request);

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        // HEARTBEAT
        sub.awaitEvent(2);
        assertThat(sim.getCallsReceived()).hasSize(1);
        assertThat(sim.getCallsReceived()).contains(SUBSCRIBE);
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void onlyOneSubscriptionPerServer() throws Exception {
        final MesosSchedulerSimulation<String, String> sim = new MesosSchedulerSimulation<>(
            Observable.just(SUBSCRIBED),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            SUBSCRIBE::equals
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> subscribe = createRequest(uri, SUBSCRIBE);

        final Observable<byte[]> observable = httpClient.submit(subscribe)
            .flatMap(response -> {
                assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.OK);
                return response.getContent();
            })
            .map(MesosSchedulerSimulationScenariosTest::bufToBytes)
            ;

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        sim.awaitSubscribeCall();
        final HttpClientResponse<ByteBuf> secondSubscribe = httpClient.submit(subscribe)
            .toBlocking()
            .first();

        assertThat(secondSubscribe.getStatus()).isEqualTo(HttpResponseStatus.CONFLICT);

        sim.awaitSendingEvents();
        sub.awaitEvent();
        final List<byte[]> expected = newArrayList(
            RecordIOUtils.createChunk(StringMessageCodec.UTF8_STRING.encode(SUBSCRIBED))
        );
        final List<byte[]> actual = testSubscriber.getOnNextEvents();
        assertThat(deepEquals(actual, expected)).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void server400_unableToDecodeMessage() throws Exception {
        final MessageCodec<String> receiveCodec = new MessageCodec<String>() {
            @NotNull
            @Override
            public byte[] encode(@NotNull final String message) {
                return new byte[0];
            }

            @NotNull
            @Override
            public String decode(@NotNull final byte[] bytes) {
                throw new RuntimeException("not implemented");
            }

            @NotNull
            @Override
            public String decode(@NotNull final InputStream in) {
                throw new RuntimeException("not implemented");
            }

            @NotNull
            @Override
            public String mediaType() {
                return "*/*";
            }

            @NotNull
            @Override
            public String show(@NotNull final String message) {
                return message;
            }
        };
        final MesosSchedulerSimulation<String, String> sim = new MesosSchedulerSimulation<>(
            Observable.just(SUBSCRIBED),
            StringMessageCodec.UTF8_STRING,
            receiveCodec,
            SUBSCRIBE::equals
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();

        final HttpClientRequest<ByteBuf> request = HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", StringMessageCodec.UTF8_STRING.mediaType())
            .withHeader("Accept", StringMessageCodec.UTF8_STRING.mediaType())
            .withContent("{\"isProto\":false}");

        final HttpClientResponse<ByteBuf> response = httpClient.submit(request)
            .toBlocking()
            .last();

        assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        final HttpResponseHeaders headers = response.getHeaders();
        assertThat(headers.getHeader("Accept")).isEqualTo(receiveCodec.mediaType());

        assertThat(sim.getCallsReceived()).hasSize(0);
    }

    @NotNull
    private static Observable<byte[]> openStream(
        @NotNull final HttpClient<ByteBuf, ByteBuf> httpClient,
        @NotNull final HttpClientRequest<ByteBuf> request
    ) {
        return httpClient.submit(request)
            .flatMap(response -> {
                assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.OK);
                final HttpResponseHeaders headers = response.getHeaders();
                assertThat(headers.getHeader("Transfer-Encoding")).isEqualTo("chunked");
                assertThat(headers.getHeader("Accept")).isEqualTo(StringMessageCodec.UTF8_STRING.mediaType());

                return response.getContent();
            })
            .map(MesosSchedulerSimulationScenariosTest::bufToBytes)
            .onBackpressureBuffer();
    }

    @NotNull
    private static HttpClient<ByteBuf, ByteBuf> createClient(@NotNull final URI uri) {
        return RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(uri.getHost(), uri.getPort())
            .pipelineConfigurator(new HttpClientPipelineConfigurator<>())
            .build();
    }

    @NotNull
    private static HttpClientRequest<ByteBuf> createRequest(@NotNull final URI uri, @NotNull final String subscribe) {
        return HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", StringMessageCodec.UTF8_STRING.mediaType())
            .withHeader("Accept", StringMessageCodec.UTF8_STRING.mediaType())
            .withContent(StringMessageCodec.UTF8_STRING.encode(subscribe));
    }

    @NotNull
    private static byte[] bufToBytes(@NotNull final ByteBuf buf) {
        final int i = buf.readableBytes();
        final byte[] data = new byte[i];
        buf.readBytes(data);
        return data;
    }

}
