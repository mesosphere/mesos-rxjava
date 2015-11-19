package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    public String toString() {
        if (details != null) {
            return String.format("%s/%s (%s)", name, version, details);
        } else {
            return String.format("%s/%s", name, version);
        }
    }
}
