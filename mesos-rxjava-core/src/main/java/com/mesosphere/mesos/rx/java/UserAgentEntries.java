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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.function.Function;

/**
 * A set of utility methods that can be used to easily create {@link UserAgentEntry} objects.
 */
public final class UserAgentEntries {
    public static final Logger LOGGER = LoggerFactory.getLogger(UserAgent.class);

    private UserAgentEntries() {}

    @NotNull
    public static Function<Class<?>, UserAgentEntry> literal(@NotNull final String name, @NotNull final String version) {
        return (Class<?> c) -> new UserAgentEntry(name, version);

    }

    @NotNull
    public static Function<Class<?>, UserAgentEntry> literal(
        @NotNull final String name,
        @NotNull final String version,
        @Nullable final String details
        ) {
        return (Class<?> c) -> new UserAgentEntry(name, version, details);

    }

    @NotNull
    public static Function<Class<?>, UserAgentEntry> userAgentEntryForGradleArtifact(@NotNull final String artifactId) {
        return (Class<?> c) -> {
            final Properties props = loadProperties(c, String.format("/META-INF/%s.properties", artifactId));
            return new UserAgentEntry(props.getProperty("artifactId", artifactId), props.getProperty("Implementation-Version", "unknown-version"));
        };
    }

    @NotNull
    public static Function<Class<?>, UserAgentEntry> userAgentEntryForMavenArtifact(@NotNull final String groupId, @NotNull final String artifactId) {
        return (Class<?> c) -> {
            final Properties props = loadProperties(c, String.format("/META-INF/maven/%s/%s/pom.properties", groupId, artifactId));
            return new UserAgentEntry(props.getProperty("artifactId", artifactId), props.getProperty("version", "unknown-version"));
        };
    }

    @NotNull
    private static Properties loadProperties(@NotNull final Class c, @NotNull final String resourcePath) {
        final Properties props = new Properties();
        try {
            final InputStream resourceAsStream = c.getResourceAsStream(resourcePath);
            if (resourceAsStream != null) {
                props.load(resourceAsStream);
                resourceAsStream.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to load classpath resources " + resourcePath, e);
        }
        return props;
    }
}
