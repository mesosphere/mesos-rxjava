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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Subscriber;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

/**
 * An {@link Operator} that can be applied to a stream of {@link ByteBuf} and produce
 * a stream of {@code byte[]} messages following the RecordIO format.
 *
 * @see <a href="http://someplace_for_recordio_spec" target="_blank">RecordIO</a>
 * @see rx.Observable#lift(Operator)
 */
public final class RecordIOOperator implements Operator<byte[], ByteBuf> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscriber<ByteBuf> call(final Subscriber<? super byte[]> subscriber) {
        return new RecordIOSubscriber(subscriber);
    }

    /**
     * A {@link Subscriber} that can process the contents of a {@link ByteBuf} and emit 0-to-many
     * {@code byte[]} messages. The format of the data stream represented by the {@link ByteBuf}
     * is in RecordIO format <a href="TODO: Find an actual link to the spec for this thing...">RecordIO</a>
     * If a single {@link ByteBuf} does not represent a full message, the data will be buffered
     * until a full message can be obtained.
     *
     * <i>
     *     Due to the way arrays in Java work, there is an effective limitation to message size of
     *     2gb. This is because arrays are indexed with signed 32-bit integers.
     * </i>
     */
    @VisibleForTesting
    static final class RecordIOSubscriber extends Subscriber<ByteBuf> {

        @NotNull
        final Subscriber<? super byte[]> child;

        private final @NotNull MessageStream messageStream = new MessageStream();

        RecordIOSubscriber(@NotNull final Subscriber<? super byte[]> child) {
            super(child);
            this.child = child;
        }

        @Override
        public void onStart() {
            request(Long.MAX_VALUE);
        }

        /**
         * When a {@link ByteBuf} is passed into this method it is completely "drained".
         * Meaning all bytes are read from it and any message(s) contained in it will be
         * extracted and then sent to the child via {@link Subscriber#onNext(Object)}.
         * If any error is encountered (exception) {@link RecordIOSubscriber#onError(Throwable)}
         * will be called and the method will terminate without attempting to do any
         * sort of recovery.
         *
         * @param buf The {@link ByteBuf} to process
         */
        @Override
        public void onNext(final ByteBuf buf) {
            try {
                messageStream.append(buf);

                byte[] message;
                while ((message = messageStream.next()) != null) {
                    child.onNext(message);
                }
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void onError(final Throwable e) {
            child.onError(e);
        }

        @Override
        public void onCompleted() {
            child.onCompleted();
        }

    }


    private static final class MessageStream {

        private static final @NotNull Logger LOGGER = LoggerFactory.getLogger(MessageStream.class);

        /** Maximum number of base-10 digits in a uint64 */
        private static final int MESSAGE_SIZE_MAX_LENGTH = 20;

        private @NotNull InputStream concatenatedInput = new ByteArrayInputStream(new byte[0]);

        private int messageSizeBytesRead = 0;

        /**
         * The message size from the stream is provided as a base-10 String representation of an
         * unsigned 64 bit integer (uint64). Since there is a possibility (since the spec isn't
         * formal on this, and the HTTP chunked Transfer-Encoding applied to the data stream can
         * allow chunks to be any size) this field functions as the bytes that have been read
         * since the end of the last message. When the next '\n' is encountered in the byte
         * stream, these bytes are converted to a UTF-8 String. This string representation is
         * then read as a {@code long} using {@link Long#valueOf(String, int)}.
         */
        private final @NotNull byte[] messageSizeBytes = new byte[MESSAGE_SIZE_MAX_LENGTH];

        /**
         * The number of bytes in the encoding is specified as an unsigned (uint64)
         * However, since arrays in java are addressed and indexed by int we drop the
         * precision early so that working with the arrays is easier.
         * Also, a byte[Integer.MAX_VALUE] is 2gb so I seriously doubt we'll be receiving
         * a message that large.
         */
        private int remainingBytesForMessage = 0;

        /**
         * The allocated {@code byte[]} for the current message being read from the stream.
         * It is null until the first message is read, and is reassigned for each new message.
         */
        private byte[] messageBytes;

        /**
         * Appends the contents of {@code buf} to the end of this stream.
         *
         * @param buf The {@link ByteBuf} to append.
         */
        public void append(final ByteBuf buf) {
            concatenatedInput = new SequenceInputStream(concatenatedInput, new ByteBufInputStream(buf));
        }

        /**
         * Gets the next message from this stream, if possible.
         *
         * @return the next complete message from the front of this stream, or {@code null} if not enough data is
         *     available.
         * @throws IOException if an error occurs when reading data from the appended {@link ByteBuf}s.
         */
        public byte[] next() throws IOException {
            if (remainingBytesForMessage == 0) {

                while (true) {
                    int i = concatenatedInput.read();
                    if (i == -1) {
                        return null;
                    }

                    byte b = (byte) i;
                    if (b == (byte) '\n') {
                        break;
                    }

                    try {
                        messageSizeBytes[messageSizeBytesRead++] = b;
                    } catch (ArrayIndexOutOfBoundsException e) {
                        String error = "Message size field exceeds limit of " + messageSizeBytes.length + " bytes";
                        throw new IllegalStateException(error, e);
                    }
                }

                final String messageSizeString = new String(messageSizeBytes, 0, messageSizeBytesRead, Charsets.UTF_8);
                final long messageSize = Long.valueOf(messageSizeString);
                messageSizeBytesRead = 0;

                if (messageSize > Integer.MAX_VALUE) {
                    LOGGER.warn("specified message size ({}) is larger than Integer.MAX_VALUE. Value will be truncated to int");
                    remainingBytesForMessage = Integer.MAX_VALUE;
                    // TODO: Possibly make this more robust to account for things larger than 2g
                } else {
                    remainingBytesForMessage = (int) messageSize;
                }

                messageBytes = new byte[remainingBytesForMessage];
            }

            final int startIndex = messageBytes.length - remainingBytesForMessage;
            final int bytesRead = concatenatedInput.read(messageBytes, startIndex, remainingBytesForMessage);

            if (bytesRead == -1) {
                concatenatedInput = new ByteArrayInputStream(new byte[0]);
                return null;
            }

            remainingBytesForMessage -= bytesRead;
            return (remainingBytesForMessage == 0) ? messageBytes : null;
        }

    }

}
