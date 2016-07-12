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

package com.mesosphere.mesos.rx.java.util;

import org.jetbrains.annotations.Nullable;

/**
 * A collection of utility methods replicating functionality provided by the {@code Preconditions} class in Guava.
 * These methods are replicated here so that our projects production libraries don't have a dependency on Guava.
 */
public class Validations {

    /**
     * Check that {@code ref} is non-null, throw a {@link NullPointerException} otherwise

     * @param ref    The reference to check for nullity, and be returned if non-null
     * @param <T>    The type of ref
     * @return       {@code ref} if non-null
     * @throws NullPointerException if {@code ref} is null
     */
    public static <T> T checkNotNull(@Nullable T ref) {
        return checkNotNull(ref, null);
    }

    /**
     * Check that {@code ref} is non-null, throw a {@link NullPointerException} otherwise
     *
     * @param ref          The reference to check for nullity, and be returned if non-null
     * @param errorMessage The error message to set on the {@link NullPointerException}
     * @param <T>          The type of ref
     * @return {@code ref} if non-null
     * @throws NullPointerException if {@code ref} is null
     */
    public static <T> T checkNotNull(@Nullable T ref, @Nullable final String errorMessage) {
        if (ref == null) {
            final String nonNullErrorMessage = String.valueOf(errorMessage); // if null, the string will be "null"
            throw new NullPointerException(nonNullErrorMessage);
        }
        return ref;
    }

}
