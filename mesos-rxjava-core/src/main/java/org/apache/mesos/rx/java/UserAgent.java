package org.apache.mesos.rx.java;

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
