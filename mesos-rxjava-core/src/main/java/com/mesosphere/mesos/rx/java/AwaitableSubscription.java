/*
 *    Copyright (C) 2015 Mesosphere, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mesosphere.mesos.rx.java;

import rx.Subscriber;
import rx.Subscription;

/**
 * A sub-interface of {@link Subscription} that provides the definition of a subscription that can be
 * awaited.
 * <p>
 * This is useful for an application that wants to start up an event stream and block its main thread
 * until the event stream has completed.
 */
public interface AwaitableSubscription extends Subscription {

    /**
     * Blocks the current thread until the underlying {@link rx.Observable} ends.
     * If the {@code rx.Observable} ends due to  {@link Subscriber#onError(Throwable) onError} the {@code Throwable}
     * delivered to {@code onError} will be rethrown. If the {@code rx.Observable} ends by
     * {@link Subscriber#onCompleted() onCompleted} the method will complete cleanly.
     * @throws Throwable If the stream terminates due to any Throwable, it will be re-thrown from this method
     */
    void await() throws Throwable;

}
