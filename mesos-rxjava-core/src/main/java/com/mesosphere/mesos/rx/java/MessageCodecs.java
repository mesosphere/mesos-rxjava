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

import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

/**
 * Utilities for working with {@link MessageCodec} instances.
 */
public final class MessageCodecs {

    /** A {@link com.mesosphere.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Call}. */
    public static final MessageCodec<Protos.Call> PROTOS_CALL = new ProtoCodec<>(Protos.Call::parseFrom);

    /** A {@link com.mesosphere.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Event}. */
    public static final MessageCodec<Protos.Event> PROTOS_EVENT = new ProtoCodec<>(Protos.Event::parseFrom);

    @NotNull
    public static final MessageCodec<String> UTF8_STRING = new StringMessageCodec();

    private MessageCodecs() {}

    /**
     * UTF_8 String codec
     * @see StandardCharsets#UTF_8
     */
    private static final class StringMessageCodec implements MessageCodec<String> {
        @NotNull
        @Override
        public byte[] encode(@NotNull final String message) {
            return message.getBytes(StandardCharsets.UTF_8);
        }

        @NotNull
        @Override
        public String decode(@NotNull final byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @NotNull
        @Override
        public String mediaType() {
            return "text/plain";
        }

        @NotNull
        @Override
        public String show(@NotNull final String message) {
            return message;
        }
    }

}
