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

package com.mesosphere.mesos.rx.java.test;

import com.google.common.io.CharStreams;
import com.mesosphere.mesos.rx.java.util.MessageCodec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * {@link MessageCodec} for a {@link StandardCharsets#UTF_8 UTF-8} String
 * @see StandardCharsets#UTF_8
 */
public final class StringMessageCodec implements MessageCodec<String> {

    @NotNull
    public static final MessageCodec<String> UTF8_STRING = new StringMessageCodec();

    private StringMessageCodec() {}

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
    public String decode(@NotNull final InputStream in) {
        try {
            return CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public String mediaType() {
        return "text/plain;charset=utf-8";
    }

    @NotNull
    @Override
    public String show(@NotNull final String message) {
        return message;
    }
}
