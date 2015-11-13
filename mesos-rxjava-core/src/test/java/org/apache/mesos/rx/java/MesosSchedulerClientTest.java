package org.apache.mesos.rx.java;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpRequestHeaders;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.mesos.rx.java.UserAgentEntries.literal;
import static org.apache.mesos.rx.java.UserAgentEntries.userAgentEntryForGradleArtifact;
import static org.apache.mesos.rx.java.UserAgentEntries.userAgentEntryForMavenArtifact;
import static org.apache.mesos.v1.Protos.*;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosSchedulerClientTest {

    public static final Protos.Call ACK = ProtoUtils.ackUpdate(
        FrameworkID.newBuilder().setValue("fwId").build(),
        ByteString.copyFromUtf8("uuid"),
        AgentID.newBuilder().setValue("agentId").build(),
        TaskID.newBuilder().setValue("taskId").build()
    );

    @Test
    public void testUserAgentGenerationIsCorrect() throws Exception {
        final String clientName = "unit-tests";
        final MesosSchedulerClient<Protos.Call, Protos.Event> client = MesosSchedulerClient.usingProtos(
            URI.create("http://localhost:12345"),
            literal(clientName, "latest")
        );

        final HttpClientRequest<ByteBuf> request = client.createPost
            .call(ACK)
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("User-Agent");
        final String ua = headers.get("User-Agent");
        assertThat(ua).startsWith(String.format("%s/%s", clientName, "latest"));
        assertThat(ua).contains("mesos-rxjava-core/");
    }

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
    public void testRequestUriFromPassedUri() throws Exception {
        final MesosSchedulerClient<Protos.Call, Protos.Event> client = MesosSchedulerClient.usingProtos(
            URI.create("http://localhost:12345/glavin/api/v1/scheduler"),
            literal("testing", "latest")
        );

        final HttpClientRequest<ByteBuf> request = client.createPost.call(ACK)
            .toBlocking()
            .first();

        assertThat(request.getUri()).isEqualTo("/glavin/api/v1/scheduler");
    }

    @Test
    public void testBasicAuthHeaderAddedToRequestWhenUserInfoPresentInUri() throws Exception {
        final MesosSchedulerClient<Protos.Call, Protos.Event> client = MesosSchedulerClient.usingProtos(
            URI.create("http://testuser:testpassword@localhost:12345/api/v1/scheduler"),
            literal("testing", "latest")
        );

        final HttpClientRequest<ByteBuf> request = client.createPost.call(ACK)
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("Authorization");
        final String authorization = headers.get("Authorization");
        assertThat(authorization).isEqualTo("Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk");

        final String base64UserPass = authorization.substring("Basic ".length());
        final String userPass = new String(Base64.getDecoder().decode(base64UserPass));
        assertThat(userPass).isEqualTo("testuser:testpassword");
    }

    @NotNull
    private static Map<String, String> headersToMap(@NotNull final HttpRequestHeaders headers) {
        final HashMap<String, String> map = Maps.newHashMap();
        for (Map.Entry<String, String> entry : headers.entries()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }
}
