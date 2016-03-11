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

import com.mesosphere.mesos.rx.java.test.RecordIOUtils;
import com.mesosphere.mesos.rx.java.util.CollectionUtils;
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
        final byte[] chunk = RecordIOUtils.createChunk(TestingProtos.SUBSCRIBED.toByteArray());
        final List<byte[]> bytes = RecordIOOperatorTest.partitionIntoArraysOfSize(chunk, chunkSize);
        final List<ByteBuf> chunks = CollectionUtils.listMap(bytes, Unpooled::copiedBuffer);

        final List<Event> events = RecordIOOperatorTest.runTestOnChunks(chunks);
        final List<Event.Type> eventTypes = CollectionUtils.listMap(events, Event::getType);

        assertThat(eventTypes).isEqualTo(newArrayList(Event.Type.SUBSCRIBED));
    }

}
