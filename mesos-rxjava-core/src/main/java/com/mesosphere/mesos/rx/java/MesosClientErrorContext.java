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

import com.google.common.base.Joiner;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents the context of a request that is made to Mesos that resulted in an error.
 * Including the status code of the HTTP Request, response headers and content body message if
 * present in the response.
 */
public final class MesosClientErrorContext {
    private static final Joiner MAP_JOINER = Joiner.on(",").skipNulls();

    private final int statusCode;
    private final String message;
    private final List<Map.Entry<String, String>> headers;

    public MesosClientErrorContext(final int statusCode, final List<Map.Entry<String, String>> headers) {
        this(statusCode, "", headers);
    }

    public MesosClientErrorContext(final int statusCode, final String message, final List<Map.Entry<String, String>> headers) {
        this.statusCode = statusCode;
        this.message = message;
        this.headers = headers;
    }

    /**
     * @return The statusCode from the HTTP response
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * @return The content body message if present in the response
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return The headers returned in the HTTP response
     */
    public List<Map.Entry<String, String>> getHeaders() {
        return headers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MesosClientErrorContext that = (MesosClientErrorContext) o;
        return statusCode == that.statusCode &&
            Objects.equals(message, that.message) &&
            Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, message, headers);
    }

    @Override
    public String toString() {
        return "MesosClientErrorContext{" +
            "statusCode=" + statusCode +
            ", message='" + message + '\'' +
            ", headers=" + MAP_JOINER.join(headers) +
            '}';
    }
}
