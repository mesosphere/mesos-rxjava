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
import com.mesosphere.mesos.rx.java.test.TestingProtos;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.*;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.Observable;
import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.subjects.BehaviorSubject;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.mesosphere.mesos.rx.java.util.CollectionUtils.deepEquals;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosSchedulerSimulationScenariosTest {

    @Rule
    public Timeout timeoutRule = new Timeout(5000, MILLISECONDS);

    @Rule
    public Async async = new Async();

    @Test
    public void awaitServerCallsReceived() throws Exception {
        final List<Protos.Event> events = newArrayList(
            TestingProtos.SUBSCRIBED,
            TestingProtos.HEARTBEAT,
            TestingProtos.OFFER
        );
        final List<byte[]> expected = events.stream()
            .map(RecordIOUtils::eventToChunk)
            .collect(Collectors.toList());

        final MesosSchedulerSimulation sim = new MesosSchedulerSimulation(Observable.from(events));
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> subscribe = createRequest(uri, TestingProtos.SUBSCRIBE);

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

            final HttpClientRequest<ByteBuf> decline = createRequest(uri, TestingProtos.DECLINE_OFFER);
            final HttpClientResponse<ByteBuf> declineResult =
                httpClient.submit(decline)
                    .toBlocking()
                    .first();

            assertThat(declineResult.getStatus()).isEqualTo(HttpResponseStatus.ACCEPTED);
        });

        sim.awaitCall(2);
        assertThat(sim.getCallsReceived()).hasSize(2);
        assertThat(sim.getCallsReceived()).isEqualTo(
            newArrayList(TestingProtos.SUBSCRIBE, TestingProtos.DECLINE_OFFER)
        );
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void behaviorSubjectSubscribeHeartbeat() throws Exception {
        final List<Protos.Event> events = newArrayList(
            TestingProtos.SUBSCRIBED,
            TestingProtos.HEARTBEAT
        );
        final List<byte[]> expected = events.stream()
            .map(RecordIOUtils::eventToChunk)
            .collect(Collectors.toList());

        /*
         * Use of BehaviorSubject here is important. The reason is that BehaviorSubject will buffer any items passed
         * to it's onNext method even if the Subject has not yet been subscribed to. A PublishSubject will only forward
         * events to it's subscriber without any buffering.
         */
        final BehaviorSubject<Protos.Event> subject = BehaviorSubject.create();
        final MesosSchedulerSimulation sim = new MesosSchedulerSimulation(subject);
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> request = createRequest(uri, TestingProtos.SUBSCRIBE);

        final Observable<byte[]> observable = openStream(httpClient, request);

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        // SUBSCRIBED
        subject.onNext(events.get(0));
        sub.awaitEvent();
        final List<byte[]> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).hasSize(1);
        assertThat(onNextEvents.get(0)).isEqualTo(expected.get(0));

        // HEARTBEAT
        subject.onNext(events.get(1));
        sub.awaitEvent();
        final List<byte[]> onNextEvents2 = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents2).hasSize(2);
        assertThat(onNextEvents2.get(1)).isEqualTo(expected.get(1));

        assertThat(sim.getCallsReceived()).hasSize(1);
        assertThat(sim.getCallsReceived()).contains(TestingProtos.SUBSCRIBE);
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void observableSubscribeHeartbeat() throws Exception {
        final List<Protos.Event> events = newArrayList(
            TestingProtos.SUBSCRIBED,
            TestingProtos.HEARTBEAT
        );
        final List<byte[]> expected = events.stream()
            .map(RecordIOUtils::eventToChunk)
            .collect(Collectors.toList());

        final MesosSchedulerSimulation sim = new MesosSchedulerSimulation(Observable.from(events));
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> request = createRequest(uri, TestingProtos.SUBSCRIBE);

        final Observable<byte[]> observable = openStream(httpClient, request);

        final TestSubscriber<byte[]> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<byte[]> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        assertThat(sim.getCallsReceived()).hasSize(0);
        assertThat(testSubscriber.getOnNextEvents()).hasSize(0);

        final Subscription subscription = observable.subscribe(sub);

        // HEARTBEAT
        sub.awaitEvent(2);
        assertThat(sim.getCallsReceived()).hasSize(1);
        assertThat(sim.getCallsReceived()).contains(TestingProtos.SUBSCRIBE);
        assertThat(deepEquals(expected, testSubscriber.getOnNextEvents())).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
    }

    @Test
    public void onlyOneSubscriptionPerServer() throws Exception {
        final MesosSchedulerSimulation sim = new MesosSchedulerSimulation(
            Observable.just(TestingProtos.SUBSCRIBED)
        );
        final int serverPort = sim.start();
        final URI uri = URI.create(String.format("http://localhost:%d/api/v1/scheduler", serverPort));
        final HttpClient<ByteBuf, ByteBuf> httpClient = createClient(uri);
        final HttpClientRequest<ByteBuf> subscribe = createRequest(uri, TestingProtos.SUBSCRIBE);

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
        final List<byte[]> expected = newArrayList(RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED));
        final List<byte[]> actual = testSubscriber.getOnNextEvents();
        assertThat(deepEquals(actual, expected)).isTrue();
        subscription.unsubscribe();
        sim.shutdown();
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
                assertThat(headers.getHeader("Accept")).isEqualTo("application/x-protobuf");

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
    private static HttpClientRequest<ByteBuf> createRequest(@NotNull final URI uri, @NotNull final Protos.Call subscribe) {
        return HttpClientRequest.createPost(uri.getPath())
            .withHeader("Content-Type", "application/x-protobuf")
            .withHeader("Accept", "application/x-protobuf")
            .withContent(subscribe.toByteArray());
    }

    @NotNull
    private static byte[] bufToBytes(@NotNull final ByteBuf buf) {
        final int i = buf.readableBytes();
        final byte[] data = new byte[i];
        buf.readBytes(data);
        return data;
    }

}
