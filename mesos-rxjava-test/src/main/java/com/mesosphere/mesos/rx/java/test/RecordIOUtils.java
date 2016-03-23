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

package com.mesosphere.mesos.rx.java.test;

import com.google.common.base.Charsets;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A set of utilities for dealing with the RecordIO format.
 * @see <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md#recordio-response-format" target="_blank">RecordIO</a>
 */
public final class RecordIOUtils {
    private static final byte NEW_LINE_BYTE = Charsets.UTF_8.encode("\n").array()[0];
    private static final int NEW_LINE_BYTE_SIZE = 1;

    private RecordIOUtils() {}

    @NotNull
    public static byte[] createChunk(@NotNull final byte[] bytes) {
        checkNotNull(bytes, "bytes must not be null");
        final byte[] messageSize = Charsets.UTF_8.encode(Integer.toString(bytes.length)).array();

        final int messageSizeLength = messageSize.length;
        final int chunkSize = messageSizeLength + NEW_LINE_BYTE_SIZE + bytes.length;
        final byte[] chunk = new byte[chunkSize];
        System.arraycopy(messageSize, 0, chunk, 0, messageSizeLength);
        chunk[messageSizeLength] = NEW_LINE_BYTE;
        System.arraycopy(bytes, 0, chunk, messageSizeLength + 1, bytes.length);
        return chunk;
    }

}
