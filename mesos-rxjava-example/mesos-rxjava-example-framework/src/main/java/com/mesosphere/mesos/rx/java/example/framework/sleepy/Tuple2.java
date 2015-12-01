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

package com.mesosphere.mesos.rx.java.example.framework.sleepy;

import org.jetbrains.annotations.NotNull;

final class Tuple2<T1, T2> {
    @NotNull
    public final T1 _1;
    @NotNull
    public final T2 _2;

    public Tuple2(@NotNull final T1 v1, @NotNull final T2 v2) {
        _1 = v1;
        _2 = v2;
    }

    public static <T1, T2> Tuple2<T1, T2> create(@NotNull final T1 v1, @NotNull final T2 v2) {
        return new Tuple2<>(v1, v2);
    }

    public static <T1, T2> Tuple2<T1, T2> t(@NotNull final T1 v1, @NotNull final T2 v2) {
        return create(v1, v2);
    }
}
