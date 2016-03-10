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

package com.mesosphere.mesos.rx.java.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implements {@link MessageCodec} for Protocol Buffers.
 *
 * @param <T> the protobuf message type
 */
public final class ProtoCodec<T extends Message> implements MessageCodec<T> {

    @NotNull
    private final ByteArrayParser<T> byteArrayParser;
    @NotNull
    private final InputStreamParser<T> inputStreamParser;

    /**
     * Instantiates a ProtoCodec instance that deserializes messages with the given
     * {@link ByteArrayParser}.
     * <p>
     * The specific parser that is provided defines which protobuf message class this codec is for. For example,
     * {@code new ProtoCodec<>(Protos.Event::parseFrom)} instantiates a codec for
     * {@link org.apache.mesos.v1.scheduler.Protos.Event Event} messages.
     *
     * @param byteArrayParser       The function to use to parse {@code byte[]}
     * @param inputStreamParser     The function to use to parse {@link InputStream}
     */
    public ProtoCodec(
        @NotNull final ByteArrayParser<T> byteArrayParser,
        @NotNull final InputStreamParser<T> inputStreamParser
    ) {
        this.byteArrayParser = byteArrayParser;
        this.inputStreamParser = inputStreamParser;
    }

    @NotNull
    @Override
    public byte[] encode(@NotNull final T message) {
        return message.toByteArray();
    }

    @NotNull
    @Override
    public T decode(@NotNull byte[] bytes) {
        try {
            return byteArrayParser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public T decode(@NotNull final InputStream in) {
        try {
            return inputStreamParser.parseFrom(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public String mediaType() {
        return "application/x-protobuf";
    }

    @NotNull
    @Override
    public String show(@NotNull final T message) {
        return ProtoUtils.protoToString(message);
    }

    /**
     * Mandatory functional interface definition due to protobuf parsing methods throwing
     * {@link InvalidProtocolBufferException}, a checked exception.
     * <p>
     * This interface should be satisfied by {@code M::parseFrom} for any {@link Message} subclass M.
     */
    @FunctionalInterface
    public interface ByteArrayParser<R> {
        @NotNull R parseFrom(@NotNull final byte[] data) throws InvalidProtocolBufferException;
    }

    /**
     * Mandatory functional interface definition due to protobuf parsing methods throwing
     * a checked exception.
     * <p>
     * This interface should be satisfied by {@code M::parseFrom} for any {@link Message} subclass M.
     */
    @FunctionalInterface
    public interface InputStreamParser<R> {
        @NotNull R parseFrom(@NotNull final InputStream data) throws IOException;
    }

}
