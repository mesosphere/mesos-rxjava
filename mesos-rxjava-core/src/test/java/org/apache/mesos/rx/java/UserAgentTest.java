package org.apache.mesos.rx.java;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.apache.mesos.rx.java.UserAgentEntries.literal;
import static org.apache.mesos.rx.java.UserAgentEntries.userAgentEntryForGradleArtifact;
import static org.apache.mesos.rx.java.UserAgentEntries.userAgentEntryForMavenArtifact;
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

}
