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

import com.google.common.collect.Maps;
import com.mesosphere.mesos.rx.java.test.StringMessageCodec;
import com.mesosphere.mesos.rx.java.util.UserAgent;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpRequestHeaders;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;

import java.net.URI;
import java.util.*;

import static com.mesosphere.mesos.rx.java.util.UserAgentEntries.literal;
import static org.assertj.core.api.Assertions.assertThat;

public final class MesosClientTest {

    @Test
    public void testUserAgentContains_MesosRxJavaCore_RxNetty() throws Exception {
        final String clientName = "unit-tests";
        final MesosClient<String, String> client = MesosClientBuilder.<String, String>newBuilder()
            .sendCodec(StringMessageCodec.UTF8_STRING)
            .receiveCodec(StringMessageCodec.UTF8_STRING)
            .mesosUri(URI.create("http://localhost:12345"))
            .applicationUserAgentEntry(literal(clientName, "latest"))
            .subscribe("subscribe")
            .processStream(events -> events.map(e -> Optional.empty()))
            .build();

        final HttpClientRequest<ByteBuf> request = client.createPost
            .call("ACK")
            .toBlocking()
            .first();

        final Map<String, String> headers = headersToMap(request.getHeaders());
        assertThat(headers).containsKeys("User-Agent");
        final String ua = headers.get("User-Agent");
        assertThat(ua).startsWith(String.format("%s/%s", clientName, "latest"));
        assertThat(ua).contains("mesos-rxjava-client/");
    }

    @Test
    public void testRequestUriFromPassedUri() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://localhost:12345/glavin/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            )
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
            .toBlocking()
            .first();

        assertThat(request.getUri()).isEqualTo("/glavin/api/v1/scheduler");
    }

    @Test
    public void testBasicAuthHeaderAddedToRequestWhenUserInfoPresentInUri() throws Exception {
        final Func1<String, Observable<HttpClientRequest<ByteBuf>>> createPost = MesosClient.curryCreatePost(
            URI.create("http://testuser:testpassword@localhost:12345/api/v1/scheduler"),
            StringMessageCodec.UTF8_STRING,
            StringMessageCodec.UTF8_STRING,
            new UserAgent(
                literal("testing", "latest")
            )
        );

        final HttpClientRequest<ByteBuf> request = createPost.call("something")
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
