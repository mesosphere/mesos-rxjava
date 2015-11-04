package org.apache.mesos.rx.java;

import com.google.common.base.Joiner;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class MesosClientErrorContext {
    private static final Joiner MAP_JOINER = Joiner.on(",").skipNulls();

    private final int statusCode;
    private final String message;
    private final List<Map.Entry<String, String>> headers;

    public MesosClientErrorContext(final int statusCode, final String message, final List<Map.Entry<String, String>> headers) {
        this.statusCode = statusCode;
        this.message = message;
        this.headers = headers;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getMessage() {
        return message;
    }

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
