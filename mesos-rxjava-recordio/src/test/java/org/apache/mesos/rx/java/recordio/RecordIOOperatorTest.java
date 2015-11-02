/*
 *    Copyright (C) 2015 Apache Software Foundation (ASF)
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

package org.apache.mesos.rx.java.recordio;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import rx.Subscriber;
import rx.functions.Action3;
import rx.observers.TestSubscriber;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordIOOperatorTest {

    private final List<byte[]> eventChunks = newArrayList(
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
    ).stream()
        .map(RecordIOUtils::eventToChunk)
        .collect(Collectors.toList());


    private List<ByteBuf> subscribeAndTenHeartbeats = eventChunks
        .stream()
        .map(Unpooled::copiedBuffer)
        .collect(Collectors.toList());

    private List<byte[]> subHbOffer = newArrayList(
        TestingProtos.SUBSCRIBED,
        TestingProtos.HEARTBEAT,
        TestingProtos.OFFER
    ).stream()
        .map(RecordIOUtils::eventToChunk)
        .collect(Collectors.toList());


    @Test
    public void correctlyAbleToReadEventsFromEventsBinFile() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/events.bin");

        final List<ByteBuf> chunks = new ArrayList<>();
        int len = 100;
        byte[] bytes = new byte[len];
        int read;
        while ((read = inputStream.read(bytes)) != -1) {
            if (read != len) {
                final byte[] readBytes = new byte[read];
                System.arraycopy(bytes, 0, readBytes, 0, read);
                chunks.add(Unpooled.copiedBuffer(readBytes));
            } else {
                chunks.add(Unpooled.copiedBuffer(bytes));
                bytes = new byte[len];
            }
        }

        runTestOnChunks(chunks, (subscriber, recordIOSubscriber, events) -> {
            final List<Event.Type> eventTypes = events.stream()
                .map(Event::getType)
                .collect(Collectors.toList());
            assertThat(eventTypes).isEqualTo(newArrayList(Event.Type.SUBSCRIBED,
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
        });
    }

    @Test
    public void readEvents_eventsNotSpanningMultipleChunks() throws Exception {
        runTestOnChunks(subscribeAndTenHeartbeats, (subscriber, recordIOSubscriber, events) -> {
            assertThat(events).hasSize(11);
            assertThat(events.get(0).getType()).isEqualTo(Event.Type.SUBSCRIBED);
            assertThat(events.stream().filter(e -> e.getType() == Event.Type.HEARTBEAT).count()).isEqualTo(10);
        });
    }

    @Test
    public void readEvents_eventsSpanningMultipleChunks() throws Exception {
        final byte[] allBytes = ByteArrays.concatAllChunks(eventChunks);

        final List<byte[]> newChunks = ByteArrays.partitionIntoArraysOfSize(allBytes, 10);

        final List<ByteBuf> chunks = newChunks.stream()
            .map(Unpooled::copiedBuffer)
            .collect(Collectors.toList());

        runTestOnChunks(chunks, (subscriber, recordIOSubscriber, events) -> {
            assertThat(events).hasSize(11);
            assertThat(events.get(0).getType()).isEqualTo(Event.Type.SUBSCRIBED);
            assertThat(events.stream().filter(e -> e.getType() == Event.Type.HEARTBEAT).count()).isEqualTo(10);
        });
    }

    @Test
    public void readEvents_multipleEventsInOneChunk() throws Exception {
        runTestOnChunks(newArrayList(
            Unpooled.copiedBuffer(ByteArrays.concatAllChunks(subHbOffer))
        ), (subscriber, recordIOSubscriber, events) -> {
            assertThat(events).hasSize(3);
            assertThat(events.get(0).getType()).isEqualTo(Event.Type.SUBSCRIBED);
            assertThat(events.get(1).getType()).isEqualTo(Event.Type.HEARTBEAT);
            assertThat(events.get(2).getType()).isEqualTo(Event.Type.OFFERS);
        });
    }

    @Test
    public void readEvents_singleEvent_chunkSize_60() throws Exception {
        final int numPartitions = 1;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_30() throws Exception {
        final int numPartitions = 2;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_20() throws Exception {
        final int numPartitions = 3;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_12() throws Exception {
        final int numPartitions = 5;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_06() throws Exception {
        final int numPartitions = 10;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_03() throws Exception {
        final int numPartitions = 15;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_02() throws Exception {
        final int numPartitions = 30;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    @Test
    public void readEvents_singleEvent_chunkSize_01() throws Exception {
        final int numPartitions = 60;
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        runTestOnPartitionedChunk(chunk, numPartitions);
    }

    private static void runTestOnPartitionedChunk(@NotNull final byte[] chunk, final int numPartitions) {
        // This test is stable because the message we're testing is 57 bytes in length
        // When it's length and '\n' are added is 60 bytes which is easily divisible
        // by [60, 30, 15, 10, 5, 3, 2, 1] (which there are test cases for above)
        assertThat(chunk.length % numPartitions).isEqualTo(0);
        final int partSize = chunk.length / numPartitions;
        final List<byte[]> bytes = ByteArrays.partitionIntoArraysOfSize(chunk, partSize);
        final List<ByteBuf> chunks = bytes.stream()
            .map(Unpooled::copiedBuffer)
            .collect(Collectors.toList());
        runTestOnChunks(
            chunks,
            (subscriber, recordIOSubscriber, events) -> {
                assertThat(events).hasSize(1);
                assertThat(events.get(0).getType()).isEqualTo(Event.Type.SUBSCRIBED);
            });
    }

    private static void runTestOnChunks(
        @NotNull final List<ByteBuf> chunks,
        @NotNull final Action3<Subscriber<byte[]>, RecordIOOperator.RecordIOSubscriber, List<Event>> assertions
    ) {
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
        final List<Event> events = child.getOnNextEvents().stream()
            .map((bs) -> {
                    try {
                        return Event.parseFrom(bs);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }
            )
            .collect(Collectors.toList());
        assertions.call(child, subscriber, events);
    }

}
