package org.apache.mesos.rx.java;

/**
 * This class represents a client error (HTTP 400 series) occurred while sending a request to Mesos.
 */
public final class MesosClientException extends RuntimeException {
    private final Object originalCall;
    private final MesosClientErrorContext context;

    /**
     * Constructor used to create a new instance
     * @param originalCall    The original object that was sent to Mesos.
     * @param context         The response context built from the Mesos response.
     */
    public MesosClientException(final Object originalCall, final MesosClientErrorContext context) {
        super(
            "Error while trying to send request."
                + " Status: " + context.getStatusCode()
                + " Message: '" + context.getMessage() + "'"
        );
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
