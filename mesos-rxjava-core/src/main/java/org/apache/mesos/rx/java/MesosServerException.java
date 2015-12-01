package org.apache.mesos.rx.java;

/**
 * This class represents a server error (HTTP 500 series) occurred while sending a request to Mesos.
 */
public final class MesosServerException extends RuntimeException {
    private final Object originalCall;
    private final MesosClientErrorContext context;

    public MesosServerException(final Object originalCall, final MesosClientErrorContext context) {
        this.originalCall = originalCall;
        this.context = context;
    }

    /**
     * The original object that was sent to Mesos.
     * @return The original object that was sent to Mesos.
     */
    public Object getOriginalCall() {
        return originalCall;
    }

    /**
     * The response context built from the Mesos response.
     * @return The response context built from the Mesos response.
     */
    public MesosClientErrorContext getContext() {
        return context;
    }
}
