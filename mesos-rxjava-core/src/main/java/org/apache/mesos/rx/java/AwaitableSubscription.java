package org.apache.mesos.rx.java;

import rx.Subscriber;
import rx.Subscription;

/**
 * A sub-interface of {@link Subscription} that provides the definition of a subscription that can be
 * awaited.
 * <p/>
 * This is useful for an application that wants to start up an event stream and block its main thread
 * until the event stream has completed.
 */
public interface AwaitableSubscription extends Subscription {

    /**
     * Blocks the current thread until the underlying {@link rx.Observable} ends.
     * If the {@code rx.Observable} ends due to  {@link Subscriber#onError(Throwable) onError} the {@code Throwable}
     * delivered to {@code onError} will be rethrown. If the {@code rx.Observable} ends by
     * {@link Subscriber#onCompleted() onCompleted} the method will complete cleanly.
     */
    void await() throws Throwable;

}
