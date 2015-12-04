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

import com.mesosphere.mesos.rx.java.util.UserAgent;
import org.junit.Test;

import java.util.regex.Pattern;

import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.literal;
import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForGradleArtifact;
import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.userAgentEntryForMavenArtifact;
import static org.assertj.core.api.Assertions.assertThat;

public final class UserAgentTest {

    @Test
    public void testArtifactPropertyResolutionFunctionsCorrectly_gradle() throws Exception {
        final UserAgent agent = new UserAgent(
            userAgentEntryForGradleArtifact("rxnetty")
        );
        assertThat(agent.toString()).matches(Pattern.compile("rxnetty/\\d+\\.\\d+\\.\\d+"));
    }

    @Test
    public void testArtifactPropertyResolutionFunctionsCorrectly_maven() throws Exception {
        final UserAgent agent = new UserAgent(
            userAgentEntryForMavenArtifact("io.netty", "netty-codec-http")
        );
        assertThat(agent.toString()).matches(Pattern.compile("netty-codec-http/\\d+\\.\\d+\\.\\d+\\.Final"));
    }

    @Test
    public void testEntriesOutputInCorrectOrder() throws Exception {
        final UserAgent agent = new UserAgent(
            literal("first", "1"),
            literal("second", "2"),
            literal("third", "3")
        );

        assertThat(agent.toString()).isEqualTo("first/1 second/2 third/3");
    }

    @Test
    public void testEntriesOutputInCorrectOrder_withDetails() throws Exception {
        final UserAgent agent = new UserAgent(
            literal("first", "1"),
            literal("second", "2", "details"),
            literal("third", "3")
        );

        assertThat(agent.toString()).isEqualTo("first/1 second/2 (details) third/3");
    }

    @Test
    public void unfoundResourceThrowsRuntimeException() throws Exception {
        try {
            final UserAgent agent = new UserAgent(
                userAgentEntryForGradleArtifact("something-that-does-not-exist")
            );
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo("Unable to load classpath resource /META-INF/something-that-does-not-exist.properties");
        }
    }
}
