package org.apache.mesos.rx.java.recordio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public final class RecordIOOperatorChunkSizesTest {

    @Parameters(name = "chunkSize={0}")
    public static List<Integer> chunkSizes() {
        return newArrayList(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 60);
    }

    private final int chunkSize;

    public RecordIOOperatorChunkSizesTest(final int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }

        this.chunkSize = chunkSize;
    }

    @Test
    public void test() {
        final byte[] chunk = RecordIOUtils.eventToChunk(TestingProtos.SUBSCRIBED);
        final List<byte[]> bytes = ByteArrays.partitionIntoArraysOfSize(chunk, chunkSize);
        final List<ByteBuf> chunks = RecordIOUtils.listMap(bytes, Unpooled::copiedBuffer);

        final List<Event> events = RecordIOOperatorTest.runTestOnChunks(chunks);
        final List<Event.Type> eventTypes = RecordIOUtils.listMap(events, Event::getType);

        assertThat(eventTypes).isEqualTo(newArrayList(Event.Type.SUBSCRIBED));
    }

}
