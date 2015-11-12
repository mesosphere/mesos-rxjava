package org.apache.mesos.rx.java;

import org.jetbrains.annotations.NotNull;
import rx.functions.Action0;
import rx.functions.Action1;

public final class SinkOperations {

    @NotNull
    private static final Action1<Throwable> ERROR_NO_OP = (t) -> {};
    @NotNull
    private static final Action0 COMPLETED_NO_OP = () -> {};

    private SinkOperations() {}

    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return new SinkOperation<>(thing, onCompleted, onError);
    }

    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted
    ) {
        return create(thing, onCompleted, ERROR_NO_OP);
    }

    @NotNull
    public static <T> SinkOperation<T> create(
        @NotNull final T thing,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, COMPLETED_NO_OP, onError);
    }

    @NotNull
    public static <T> SinkOperation<T> create(@NotNull final T thing) {
        return create(thing, COMPLETED_NO_OP, ERROR_NO_OP);
    }

    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, onCompleted, onError);
    }

    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action0 onCompleted
    ) {
        return create(thing, onCompleted);
    }

    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(thing, onError);
    }

    @NotNull
    public static <T> SinkOperation<T> sink(
        @NotNull final T thing
    ) {
        return create(thing);
    }


}
