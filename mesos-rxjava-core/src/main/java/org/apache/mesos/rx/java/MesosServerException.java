package org.apache.mesos.rx.java;

public final class MesosServerException extends RuntimeException {
    private final Object originalCall;
    private final MesosClientErrorContext context;

    public MesosServerException(final Object originalCall, final MesosClientErrorContext context) {
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
