package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import rx.functions.Action0;
import rx.functions.Action1;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple object that represents a {@link org.apache.mesos.v1.scheduler.Protos.Call} that is to be sent
 * to Mesos.  The current implementation of Mesos is such that any call to its HTTP API with a
 * {@link org.apache.mesos.v1.scheduler.Protos.Call} that isn't a
 * {@link org.apache.mesos.v1.scheduler.Protos.Call.Type#SUBSCRIBE} will result in a semi-blocking response.
 * <p/>
 * This means things like request validation (including body deserialization and field validation) are
 * performed synchronously during the request. Due to this behavior, this class exists to inform the
 * user of the success or failure of requests sent to the master.
 * <p/>
 * It should be noted that this object doesn't represent the means of detecting and handling connection errors
 * with Mesos as the intent is that it will be communicated to the whole event stream rather than an
 * individual {@link org.apache.mesos.v1.scheduler.Protos.Call}.
 * <p/>
 * <i>NOTE</i>
 * The semantics of which thread a callback will be invoked on are undefined and should not be relied upon.
 * This means that all standard thread safety/guards should be in place for state changes that take place
 * inside the callbacks.
 *
 * @see SinkOperations#create
 */
public final class SinkOperation<T> {
    @NotNull
    private final T thingToSink;
    @NotNull
    private final Action1<Throwable> onError;
    @NotNull
    private final Action0 onCompleted;

    /**
     * This constructor is considered an internal API and should not be used directly, instead use one of the
     * factory methods defined in {@link SinkOperations}.
     * @param thingToSink    The {@link org.apache.mesos.v1.scheduler.Protos.Call} to send to Mesos
     * @param onCompleted    The callback invoked when HTTP 202 is returned by Mesos
     * @param onError        The callback invoked for an HTTP 400 or 500 status code returned by Mesos
     */
    SinkOperation(
        @NotNull final T thingToSink,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        this.thingToSink = checkNotNull(thingToSink, "argument thingToSink can not be null");
        this.onCompleted = checkNotNull(onCompleted, "argument onCompleted can not be null");
        this.onError = checkNotNull(onError, "argument onError can not be null");
    }

    public void onCompleted() {
        onCompleted.call();
    }

    public void onError(final Throwable e) {
        onError.call(e);
    }

    @NotNull
    public T getThingToSink() {
        return thingToSink;
    }
}
