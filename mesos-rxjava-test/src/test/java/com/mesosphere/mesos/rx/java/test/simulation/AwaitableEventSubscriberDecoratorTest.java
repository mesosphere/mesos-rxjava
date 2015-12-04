/*
 *    Copyright (C) 2016 Mesosphere, Inc
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

package com.mesosphere.mesos.rx.java.test.simulation;

import com.mesosphere.mesos.rx.java.test.Async;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.subjects.BehaviorSubject;

import java.util.concurrent.TimeUnit;

public final class AwaitableEventSubscriberDecoratorTest {

    @Rule
    public Timeout timeout = new Timeout(500, TimeUnit.MILLISECONDS);

    @Rule
    public Async async = new Async();

    @Test
    public void awaitEventWorks_onNext() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<String> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        final BehaviorSubject<String> subject = BehaviorSubject.create();

        final Subscription subscription = subject.subscribe(sub);

        async.run(() -> subject.onNext("hello"));

        sub.awaitEvent();
        testSubscriber.assertValue("hello");
        testSubscriber.assertNoTerminalEvent();
        subscription.unsubscribe();
    }

    @Test
    public void awaitEventWorks_onError() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<String> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        final BehaviorSubject<String> subject = BehaviorSubject.create();

        final Subscription subscription = subject.subscribe(sub);

        final RuntimeException e = new RuntimeException("doesn't matter");
        async.run(() -> subject.onError(e));

        sub.awaitEvent();
        testSubscriber.assertNoValues();
        testSubscriber.assertError(e);
        subscription.unsubscribe();
    }

    @Test
    public void awaitEventWorks_onCompleted() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<String> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        final BehaviorSubject<String> subject = BehaviorSubject.create();

        final Subscription subscription = subject.subscribe(sub);

        async.run(subject::onCompleted);

        sub.awaitEvent();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        subscription.unsubscribe();
    }

    @Test
    public void awaitEventWorks() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final AwaitableEventSubscriberDecorator<String> sub = new AwaitableEventSubscriberDecorator<>(testSubscriber);

        final BehaviorSubject<String> subject = BehaviorSubject.create();

        final Subscription subscription = subject.subscribe(sub);

        async.run(() -> {
            subject.onNext("hello");
            subject.onNext("world");
            subject.onNext("!");
            subject.onCompleted();
        });

        sub.awaitEvent(Integer.MAX_VALUE);
        testSubscriber.assertValues("hello", "world", "!");
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        subscription.unsubscribe();
    }

}
