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

package com.mesosphere.mesos.rx.java.protobuf;

import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

/**
 * A set of utility methods that make working with Mesos Protos easier.
 */
public final class ProtoUtils {
    private static final Pattern PROTO_TO_STRING = Pattern.compile(" *\\n *");

    private ProtoUtils() {}

    /**
     * Utility method to make logging of protos a little better than what is done by default.
     * @param message    The proto message to be "stringified"
     * @return  A string representation of the provided {@code message} surrounded with curly braces ('{}') and all
     *          new lines replaced with a space.
     */
    @NotNull
    public static String protoToString(@NotNull final Object message) {
        return "{ " + PROTO_TO_STRING.matcher(message.toString()).replaceAll(" ").trim() + " }";
    }

}
