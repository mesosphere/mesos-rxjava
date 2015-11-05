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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.jetbrains.annotations.NotNull;
import rx.Observable.Operator;
import rx.Subscriber;


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

        @NotNull
        private final ConcatenatedInputStream pendingInput = new ConcatenatedInputStream();

        @NotNull
        private final MessageStream messageStream = new MessageStream(pendingInput);

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
                pendingInput.append(new ByteBufInputStream(buf));

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


}
