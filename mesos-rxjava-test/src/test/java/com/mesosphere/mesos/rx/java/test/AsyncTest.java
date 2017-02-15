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

package com.mesosphere.mesos.rx.java.test;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runners.model.MultipleFailureException;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public final class AsyncTest {

    @Test
    public void exceptionInTaskFailsTest_runtimeException() throws Throwable {
        final Async async = new Async();
        final IllegalStateException kaboom = new IllegalStateException("should fail");
        async.run(() -> {
            throw kaboom;
        });

        try {
            async.verify();
        } catch (AssertionError e) {
            assertThat(e).hasCauseExactlyInstanceOf(IllegalStateException.class);
            assertThat(e.getCause()).hasMessage("should fail");
        }
    }

    @Test
    public void exceptionInTaskFailsTest_checkedException() throws Throwable {
        final Async async = new Async();
        final IOException kaboom = new IOException("should fail");
        async.run(() -> {
            throw kaboom;
        });

        try {
            async.verify();
        } catch (AssertionError e) {
            assertThat(e).hasCauseExactlyInstanceOf(RuntimeException.class);
            assertThat(e.getCause()).hasCauseExactlyInstanceOf(IOException.class);
            assertThat(e.getCause().getCause()).hasMessage("should fail");
        }
    }

    @Test
    public void exceptionInTaskFailsTest_assertionError() throws Throwable {
        final Async async = new Async();
        async.run(() -> assertThat(false).isTrue());

        try {
            async.verify();
        } catch (AssertionError e) {
            assertThat(e).hasNoCause();
        }
    }

    @Test
    public void exceptionInTaskFailsTest_taskRunningAfterTestComplete() throws Throwable {
        final Async async = new Async();
        async.run("sleep", () -> Thread.sleep(500));

        try {
            async.verify();
        } catch (AssertionError e) {
            assertThat(e).hasNoCause();
            assertThat(e).hasMessage("Task [sleep] still running after test completion");
        }
    }

    @Test
    public void exceptionInTaskFailsTest_taskRunningAfterTestComplete_noName() throws Throwable {
        final Async async = new Async();
        async.run(() -> Thread.sleep(500));

        try {
            async.verify();
        } catch (AssertionError e) {
            assertThat(e).hasNoCause();
            assertThat(e).hasMessage("Task [Async-0] still running after test completion");
        }
    }

    @Test
    public void cleanTaskResultsInNoError() throws Throwable {
        final Async async = new Async();
        async.run(() -> assertThat(true).isTrue());

        async.verify();
    }

    @Test
    public void multipleExceptionReportedWhenTheyOccur() {
        final Async async = new Async();
        final IllegalStateException exception1 = new IllegalStateException("exception-1");
        final IllegalStateException exception2 = new IllegalStateException("exception-2");
        final IllegalStateException exception3 = new IllegalStateException("exception-3");
        async.run(() -> {
            throw exception1;
        });
        async.run(() -> {
            throw exception2;
        });
        async.run(() -> {
            throw exception3;
        });

        try {
            async.verify();
            fail("Exception should have been thrown in verify");
        } catch (MultipleFailureException mfe) {
            final Set<String> errorMessages = mfe.getFailures().stream()
                .map(Throwable::getCause)
                .map(Throwable::getMessage)
                .collect(Collectors.toSet());
            assertThat(errorMessages).isEqualTo(Sets.newHashSet("exception-1", "exception-2", "exception-3"));
            assertThat(mfe.getMessage()).contains("Error while running Async: exception-1");
        } catch (Throwable e) {
            fail("Expected MultipleFailureException but got: " + e.getClass().getName());
        }
    }
}
