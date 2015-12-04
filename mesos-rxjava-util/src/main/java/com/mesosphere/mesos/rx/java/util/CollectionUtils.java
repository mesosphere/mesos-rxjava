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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Set of utility methods that make it easier to work with certain types of Collections
 */
public final class CollectionUtils {
    private CollectionUtils() {}

    @NotNull
    public static <T, R> List<R> listMap(@NotNull final List<T> input, @NotNull final Function<T, R> mapper) {
        return input
            .stream()
            .map(mapper)
            .collect(Collectors.toList());
    }

    /**
     * The contract of {@link List#equals(Object)} is that a "deep equals" is performed, however the actual
     * "deep equals" depends on the elements being compared. In the case of arrays the default
     * {@link Object#equals(Object)} is used.
     * <p>
     * This method is a convenience utility to check if two lists of byte[] are "equal"
     * @param a    The first List of byte[]s
     * @param b    The second List of byte[]s
     * @return {@code true} if {@code a} and {@code b} are the exact same array (determined by {@code ==}) OR only if
     *         {@code a} and {@code b} are the same size AND {@link Arrays#equals(byte[], byte[])} returns {@code true}
     *         for every index matching element from {@code a} and {@code b}.
     */
    public static boolean deepEquals(@NotNull final List<byte[]> a, @NotNull final List<byte[]> b) {
        if (a == b) {
            return true;
        }
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!Arrays.equals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }
}
