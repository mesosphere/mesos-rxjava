package org.apache.mesos.rx.java;

public final class MesosClientException extends RuntimeException {
    private final Object originalCall;
    private final MesosClientErrorContext context;

    public MesosClientException(final Object originalCall, final MesosClientErrorContext context) {
        super(
            "Error while trying to send request."
                + " Status: " + context.getStatusCode()
                + " Message: '" + context.getMessage() + "'"
        );
        this.originalCall = originalCall;
        this.context = context;
    }

    public Object getOriginalCall() {
        return originalCall;
    }

    public MesosClientErrorContext getContext() {
        return context;
    }
}
