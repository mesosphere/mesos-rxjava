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

package com.mesosphere.mesos.rx.java;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.jetbrains.annotations.NotNull;

/**
 * Implements {@link MessageCodec} for Protocol Buffers.
 *
 * @param <T> the protobuf message type
 */
public final class ProtoCodec<T extends Message> implements MessageCodec<T> {

    @NotNull
    private final Parser<T> parser;

    /**
     * Instantiates a {@link ProtoCodec} instance that deserializes messages with the given
     * {@link com.mesosphere.mesos.rx.java.ProtoCodec.Parser}.
     * <p>
     * The specific parser that is provided defines which protobuf message class this codec is for. For example,
     * {@code new ProtoCodec<>(Protos.Event::parseFrom)} instantiates a codec for
     * {@link org.apache.mesos.v1.scheduler.Protos.Event} messages.
     *
     * @param parser the protobuf parsing method
     */
    public ProtoCodec(@NotNull final Parser<T> parser) {
        this.parser = parser;
    }

    /**
     * @inheritDoc
     */
    @NotNull
    @Override
    public byte[] encode(@NotNull final T message) {
        return message.toByteArray();
    }

    /**
     * @inheritDoc
     *
     * @throws RuntimeException if an error occurs when parsing the protobuf
     */
    @NotNull
    @Override
    public T decode(@NotNull byte[] bytes) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @inheritDoc
     */
    @NotNull
    @Override
    public String mediaType() {
        return "application/x-protobuf";
    }

    /**
     * @inheritDoc
     */
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
    public interface Parser<R> {
        @NotNull R parseFrom(@NotNull final byte[] data) throws InvalidProtocolBufferException;
    }

}
