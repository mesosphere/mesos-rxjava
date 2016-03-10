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

package com.mesosphere.mesos.rx.java.recordio;

import com.google.common.primitives.Bytes;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.mesos.rx.java.test.RecordIOUtils;
import com.mesosphere.mesos.rx.java.util.CollectionUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import rx.Subscriber;
import rx.observers.TestSubscriber;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordIOOperatorTest {

    private static final List<Event> EVENT_PROTOS = newArrayList(
        TestingProtos.SUBSCRIBED,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT,
        TestingProtos.HEARTBEAT
    );

    private static final List<byte[]> EVENT_CHUNKS = EVENT_PROTOS.stream()
        .map(AbstractMessageLite::toByteArray)
        .map(RecordIOUtils::createChunk)
        .collect(Collectors.toList());

    @Test
    public void correctlyAbleToReadEventsFromEventsBinFile() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/events.bin");

        final List<ByteBuf> chunks = new ArrayList<>();
        final byte[] bytes = new byte[100];

        int read;
        while ((read = inputStream.read(bytes)) != -1) {
            chunks.add(Unpooled.copiedBuffer(bytes, 0, read));
        }

        final List<Event> events = runTestOnChunks(chunks);
        final List<Event.Type> eventTypes = CollectionUtils.listMap(events, Event::getType);

        assertThat(eventTypes).isEqualTo(newArrayList(
            Event.Type.SUBSCRIBED,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.OFFERS,
            Event.Type.HEARTBEAT,
            Event.Type.HEARTBEAT,
            Event.Type.HEARTBEAT
        ));
    }

    @Test
    public void readEvents_eventsNotSpanningMultipleChunks() throws Exception {
        final List<ByteBuf> eventBufs = CollectionUtils.listMap(EVENT_CHUNKS, Unpooled::copiedBuffer);

        final List<Event> events = runTestOnChunks(eventBufs);
        assertThat(events).isEqualTo(EVENT_PROTOS);
    }

    @Test
    public void readEvents_eventsSpanningMultipleChunks() throws Exception {
        final byte[] allBytes = concatAllChunks(EVENT_CHUNKS);
        final List<byte[]> arrayChunks = partitionIntoArraysOfSize(allBytes, 10);
        final List<ByteBuf> bufChunks = CollectionUtils.listMap(arrayChunks, Unpooled::copiedBuffer);

        final List<Event> events = runTestOnChunks(bufChunks);
        assertThat(events).isEqualTo(EVENT_PROTOS);
    }

    @Test
    public void readEvents_multipleEventsInOneChunk() throws Exception {
        final List<Event> subHbOffer = newArrayList(
            TestingProtos.SUBSCRIBED,
            TestingProtos.HEARTBEAT,
            TestingProtos.OFFER
        );
        final List<byte[]> eventChunks = subHbOffer.stream()
            .map(AbstractMessageLite::toByteArray)
            .map(RecordIOUtils::createChunk)
            .collect(Collectors.toList());
        final List<ByteBuf> singleChunk = newArrayList(Unpooled.copiedBuffer(concatAllChunks(eventChunks)));

        final List<Event> events = runTestOnChunks(singleChunk);
        assertThat(events).isEqualTo(subHbOffer);
    }

    @NotNull
    static List<Event> runTestOnChunks(@NotNull final List<ByteBuf> chunks) {
        final TestSubscriber<byte[]> child = new TestSubscriber<>();
        final Subscriber<ByteBuf> call = new RecordIOOperator().call(child);

        assertThat(call).isInstanceOf(RecordIOOperator.RecordIOSubscriber.class);

        final RecordIOOperator.RecordIOSubscriber subscriber = (RecordIOOperator.RecordIOSubscriber) call;
        chunks.stream().forEach(subscriber::onNext);
        child.assertNoErrors();
        child.assertNotCompleted();
        child.assertNoTerminalEvent();
        assertThat(subscriber.messageSizeBytesBuffer).isEmpty();
        assertThat(subscriber.messageBytes).isNull();
        assertThat(subscriber.remainingBytesForMessage).isEqualTo(0);

        return CollectionUtils.listMap(child.getOnNextEvents(), (bs) -> {
            try {
                return Event.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @NotNull
    static List<byte[]> partitionIntoArraysOfSize(@NotNull final byte[] allBytes, final int chunkSize) {
        final List<byte[]> chunks = new ArrayList<>();
        final int numFullChunks = allBytes.length / chunkSize;

        int chunkStart = 0;
        int chunkEnd = 0;

        for (int i = 0; i < numFullChunks; i++) {
            chunkEnd += chunkSize;
            chunks.add(Arrays.copyOfRange(allBytes, chunkStart, chunkEnd));
            chunkStart = chunkEnd;
        }

        if (chunkStart < allBytes.length) {
            chunks.add(Arrays.copyOfRange(allBytes, chunkStart, allBytes.length));
        }

        return chunks;
    }


    @NotNull
    static byte[] concatAllChunks(@NotNull final List<byte[]> chunks) {
        return Bytes.concat(chunks.toArray(new byte[chunks.size()][]));
    }

}
