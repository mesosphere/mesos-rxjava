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
import java.util.List;

public final class ByteArrays {

    private static final byte[] ZERO_BYTES = new byte[0];

    private ByteArrays() {}

    @NotNull
    static List<byte[]> partitionIntoArraysOfSize(@NotNull final byte[] allBytes, final int partSize) {
        final List<byte[]> newChunks = new ArrayList<>();
        byte[] newChunk = null;
        for (int i = 0; i < allBytes.length; i++) {
            final byte b = allBytes[i];
            final int partIndex = i % partSize;
            if (partIndex == 0) {
                if (newChunk != null) {
                    newChunks.add(newChunk);
                }
                newChunk = new byte[partSize];
            }
            //noinspection ConstantConditions
            newChunk[partIndex] = b;

            if (i + 1 == allBytes.length) {
                newChunks.add(newChunk);
            }
        }
        return newChunks;
    }

    @NotNull
    static byte[] concatAllChunks(@NotNull final List<byte[]> chunks) {
        return chunks
            .stream()
            .reduce(ZERO_BYTES, Bytes::concat/*acc, val*/);
    }

}
