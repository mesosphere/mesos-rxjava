package org.apache.mesos.rx.java;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpRequestHeaders;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.mesos.rx.java.UserAgentEntries.literal;
import static org.apache.mesos.v1.Protos.*;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosSchedulerClientTest {

    @Test
    public void testUserAgentGenerationIsCorrect() throws Exception {
        final String clientName = "unit-tests";
        final MesosSchedulerClient<Protos.Call, Protos.Event> client = MesosSchedulerClient.usingProtos(
            "localhost",
            12345,
            literal(clientName, "latest")
        );

        final Protos.Call ack = ProtoUtils.ackUpdate(
            FrameworkID.newBuilder().setValue("fwId").build(),
            ByteString.copyFromUtf8("uuid"),
            AgentID.newBuilder().setValue("agentId").build(),
            TaskID.newBuilder().setValue("taskId").build()
        );

        final HttpClientRequest<ByteBuf> request = client.createPost
            .call(ack)
            .toBlocking()
            .first();

        assertThat(request.getUri()).isEqualTo("/api/v1/scheduler");
        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("User-Agent");
        final String ua = headers.get("User-Agent");
        assertThat(ua).startsWith(String.format("%s/%s", clientName, "latest"));
        assertThat(ua).contains("mesos-rxjava-core/");
        assertThat(ua).contains("rxnetty/");
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
