/*
 *    Copyright (C) 2015 Apache Software Foundation (ASF)
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
 *
 */

package org.apache.mesos.rx.java;

import org.junit.Test;
import rx.Observable;
import rx.observers.Subscribers;
import rx.observers.TestSubscriber;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public final class SubscriberDecoratorTest {


    @Test
    public void worksForCompleted() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);
        Observable.just("something")
            .subscribe(decorator);

        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue("something");

        assertThat(decorator.call()).isEqualTo(Optional.empty());
    }

    @Test
    public void worksForError() throws Exception {
        final ExecutorService service = Executors.newSingleThreadExecutor();
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);
        Observable.<String>from(service.submit(() -> {throw new RuntimeException("kaboom");}))
            .subscribe(decorator);

        testSubscriber.assertNoValues();
        testSubscriber.assertError(ExecutionException.class);

        final Optional<Throwable> e = decorator.call();
        assertThat(e.isPresent()).isTrue();
        final Throwable t = e.get();
        assertThat(t).isInstanceOf(ExecutionException.class);
        assertThat(t.getCause()).isInstanceOf(RuntimeException.class);
        assertThat(t.getCause().getMessage()).isEqualTo("kaboom");
    }

    @Test
    public void exceptionThrowByOnErrorIsReturned() throws Exception {
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(
            Subscribers.<String>create(
                s -> {throw new RuntimeException("Supposed to break");},
                (t) -> {throw new RuntimeException("wrapped", t);}
            )
        );
        Observable.just("doesn't matter")
            .subscribe(decorator);

        final Optional<Throwable> e = decorator.call();
        assertThat(e.isPresent()).isTrue();
        final Throwable t = e.get();
        assertThat(t).isInstanceOf(RuntimeException.class);
        assertThat(t.getMessage()).isEqualTo("wrapped");
        assertThat(t.getCause().getMessage()).isEqualTo("Supposed to break");
    }

    @Test
    public void exceptionThrowByOnCompletedIsReturned() throws Exception {
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(
            Subscribers.<String>create(
                s -> {},
                (t) -> {throw new RuntimeException("wrapped", t);},
                () -> {throw new RuntimeException("wrapped2");}
            )
        );
        Observable.just("doesn't matter")
            .subscribe(decorator);

        final Optional<Throwable> e = decorator.call();
        assertThat(e.isPresent()).isTrue();
        final Throwable t = e.get();
        assertThat(t).isInstanceOf(RuntimeException.class);
        assertThat(t.getMessage()).isEqualTo("wrapped2");
    }

    @Test(expected = StackOverflowError.class)
    public void fatalExceptionIsThrown() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);
        Observable.just("doesn't matter")
            .map((s) -> {
                //noinspection ConstantIfStatement,ConstantConditions  -- This is here to trick the compiler to think this function returns a string
                if (true) {
                    throw new StackOverflowError("stack overflow");
                }
                return s;
            })
            .subscribe(decorator);

        testSubscriber.assertNoValues();
    }

    @Test
    public void onlyOneValueWillBeHeld_completed() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);

        decorator.onCompleted();
        decorator.onCompleted();

        final Optional<Throwable> call = decorator.call();
        assertThat(call.isPresent()).isFalse();
    }

    @Test
    public void receiveFirstValue_error() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);

        decorator.onError(new RuntimeException("this one"));
        decorator.onError(new RuntimeException("not this one"));

        final Optional<Throwable> e = decorator.call();
        assertThat(e.isPresent()).isTrue();
        final Throwable t = e.get();
        assertThat(t).isInstanceOf(RuntimeException.class);
        assertThat(t.getMessage()).isEqualTo("this one");
    }

    @Test
    public void onlyOneValueWillBeHeld_completedThenError() throws Exception {
        final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        final MesosSchedulerClient.SubscriberDecorator<String> decorator = new MesosSchedulerClient.SubscriberDecorator<>(testSubscriber);

        decorator.onCompleted();
        decorator.onError(new RuntimeException("really doesn't matter"));

        final Optional<Throwable> call = decorator.call();
        assertThat(call.isPresent()).isFalse();
    }

}
