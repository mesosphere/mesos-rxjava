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

import com.google.common.primitives.Bytes;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ByteArrays {

    private static final byte[] ZERO_BYTES = new byte[0];

    private ByteArrays() {}

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
        return chunks
            .stream()
            .reduce(ZERO_BYTES, Bytes::concat);
    }

}
