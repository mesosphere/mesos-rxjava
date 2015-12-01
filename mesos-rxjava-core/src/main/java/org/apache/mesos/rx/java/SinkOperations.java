package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import rx.functions.Action0;
import rx.functions.Action1;

/**
 * This class provides the set of methods that can be used to create {@link SinkOperation}s
 */
public final class SinkOperations {

    @NotNull
    private static final Action1<Throwable> ERROR_NO_OP = (t) -> {};
    @NotNull
    private static final Action0 COMPLETED_NO_OP = () -> {};

    private SinkOperations() {}

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onCompleted    The callback to be invoked if the message is successfully sent to Mesos.
     * @param onError        The callback to be invoked if an error occurred while sending to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     */
    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return new SinkOperation<>(thing, onCompleted, onError);
    }
    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onCompleted    The callback to be invoked if the message is successfully sent to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     */
    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted
    ) {
        return create(thing, onCompleted, ERROR_NO_OP);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onError        The callback to be invoked if an error occurred while sending to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     */
    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, COMPLETED_NO_OP, onError);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     */
    @NotNull
    public static <T> SinkOperation<T> create(@NotNull final T thing) {
        return create(thing, COMPLETED_NO_OP, ERROR_NO_OP);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onCompleted    The callback to be invoked if the message is successfully sent to Mesos.
     * @param onError        The callback to be invoked if an error occurred while sending to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     * @see #create(Object, Action0, Action1)
     */
    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, onCompleted, onError);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onCompleted    The callback to be invoked if the message is successfully sent to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     * @see #create(Object, Action0)
     */
    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted
    ) {
        return create(thing, onCompleted);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param onError        The callback to be invoked if an error occurred while sending to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     * @see #create(Object, Action1)
     */
    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, onError);
    }

    /**
     * Creates a new {@link SinkOperation}.
     * @param thing          The message to be sent to Mesos.
     * @param <T>            The type of the message to be sent to Mesos.
     * @return  A new {@link SinkOperation} that can be sent to Mesos.
     * @see #create(Object)
     */
    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing
    ) {
        return create(thing);
    }


}
