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

import org.jetbrains.annotations.NotNull;

/**
 * A {@code MessageCodec<T>} defines how values of type {@code T} can be serialized to and deserialized from sequences
 * of bytes.
 * <p>
 * There should be at least one implementation of this interface for every type of message that can be sent to or
 * received from the Mesos API. For example, Mesos messages in Protocol Buffer format can be translated using
 * {@link ProtoCodec}.
 *
 * @param <T> the message type that this codec is defined for
 */
public interface MessageCodec<T> {

    /**
     * Serialize the given {@code message} into an array of bytes.
     *
     * @param message the message to serialize
     * @return the serialized message
     */
    @NotNull
    byte[] encode(@NotNull final T message);

    /**
     * Deserialize the given byte array into a message.
     *
     * @param bytes the bytes to deserialize
     * @return the deserialized message
     * @throws RuntimeException If an error occurs when decoding {@code bytes}. If a checked exception is possible
     *                          it should be wrapped in a RuntimeException.
     */
    @NotNull
    T decode(@NotNull final byte[] bytes);

    /**
     * Returns the <a target="_blank" href="https://en.wikipedia.org/wiki/Media_type">IANA media type</a> of the serialized message
     * format handled by this object.
     * <p>
     * The value returned by this method will be used in the {@code Content-Type} and {@code Accept} headers for
     * messages sent to and received from Mesos, respectively.
     * <p>
     * For example, {@link ProtoCodec} uses the media type {@code application/x-protobuf}.
     *
     * @return the media type identifier
     */
    @NotNull
    String mediaType();

    /**
     * Renders the given {@code message} to informative, human-readable text.
     * <p>
     * The intent of this method is to allow messages to be easily read in program logs and while debugging.
     *
     * @param message the message to render
     * @return the rendered message
     */
    @NotNull
    String show(@NotNull final T message);

}
