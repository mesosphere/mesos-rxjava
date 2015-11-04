package org.apache.mesos.rx.java;

import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import rx.functions.Action0;
import rx.functions.Action1;

public final class SinkOperations {
    private SinkOperations() {}

    @NotNull
    public static SinkOperation create(
        @NotNull final Protos.Call call,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return new SinkOperation(call, onCompleted, onError);
    }

    @NotNull
    public static SinkOperation create(
        @NotNull final Protos.Call call,
        @NotNull final Action0 onCompleted
    ) {
        return new SinkOperation(call, onCompleted, (t) -> {});
    }

    @NotNull
    public static SinkOperation create(
        @NotNull final Protos.Call call,
        @NotNull final Action1<Throwable> onError
    ) {
        return new SinkOperation(call, () -> {}, onError);
    }

    @NotNull
    public static SinkOperation create(
        @NotNull final Protos.Call call
    ) {
        return new SinkOperation(
            call,
            () -> {},
            (t) -> {}
        );
    }

    @NotNull
    public static SinkOperation sink(
        @NotNull final Protos.Call call,
        @NotNull final Action0 onCompleted,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(call, onCompleted, onError);
    }

    @NotNull
    public static SinkOperation sink(
        @NotNull final Protos.Call call,
        @NotNull final Action0 onCompleted
    ) {
        return create(call, onCompleted);
    }

    @NotNull
    public static SinkOperation sink(
        @NotNull final Protos.Call call,
        @NotNull final Action1<Throwable> onError
    ) {
        return create(call, onError);
    }

    @NotNull
    public static SinkOperation sink(
        @NotNull final Protos.Call call
    ) {
        return create(call);
    }


}
