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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * A specific Entry to be listed in the HTTP User-Agent header
 */
public final class UserAgentEntry {
    @NotNull
    private final String name;
    @NotNull
    private final String version;
    @Nullable
    private final String details;

    public UserAgentEntry(@NotNull final String name, @NotNull final String version) {
        this(name, version, null);
    }

    public UserAgentEntry(@NotNull final String name, @NotNull final String version, @Nullable final String details) {
        this.name = name;
        this.version = version;
        this.details = details;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public String getVersion() {
        return version;
    }

    @Nullable
    public String getDetails() {
        return details;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UserAgentEntry that = (UserAgentEntry) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(version, that.version) &&
            Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, details);
    }

    @Override
    public String toString() {
        if (details != null) {
            return String.format("%s/%s (%s)", name, version, details);
        } else {
            return String.format("%s/%s", name, version);
        }
    }
}
