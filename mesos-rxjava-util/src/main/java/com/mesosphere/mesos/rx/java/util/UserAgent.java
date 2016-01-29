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

package com.mesosphere.mesos.rx.java.util;

import com.google.common.base.Joiner;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class represents an HTTP User-Agent Header
 */
public final class UserAgent {

    private static final Joiner JOINER = Joiner.on(" ").skipNulls();

    @NotNull
    private final List<UserAgentEntry> entries;
    @NotNull
    private final String toStringValue;

    @SafeVarargs
    public UserAgent(@NotNull final Function<Class<?>, UserAgentEntry>... entries) {
        this.entries =
            Collections.unmodifiableList(
                Arrays.asList(entries)
                    .stream()
                    .map(f -> f.apply(this.getClass()))
                    .collect(Collectors.toList())
            );
        this.toStringValue = JOINER.join(this.entries);
    }

    @NotNull
    public List<UserAgentEntry> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return toStringValue;
    }

}
