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

package com.mesosphere.mesos.rx.java.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public final class ProtobufMessageCodecsTest {

    @NotNull
    private static final Protos.Event HEARTBEAT = Protos.Event.newBuilder()
        .setType(Protos.Event.Type.HEARTBEAT)
        .build();

    @NotNull
    private static final Protos.Event SUBSCRIBED = SchedulerEvents.subscribed("a7cfd25c-79bd-481c-91cc-692e5db1ec3d", 15);

    @NotNull
    private static final Protos.Event OFFER = SchedulerEvents.resourceOffer("host1", "offer1", "agent1", "frw1", 8d, 8192, 8192);

    @NotNull
    private static final byte[] SERIALIZED_HEARTBEAT = Base64.getDecoder().decode("CAg=");

    @NotNull
    private static final byte[] SERIALIZED_OFFER = Base64.getDecoder().decode(
        "CAIabApqCggKBm9mZmVyMRIGCgRmcncxGggKBmFnZW50MSIFaG9zdDEqFgoEY3B1cxAAGgkJAAAAAAAAIEAyASoqFQoDbWVtEAAaCQkAAAA" +
            "AAADAQDIBKioWCgRkaXNrEAAaCQkAAAAAAADAQDIBKg=="
    );

    @NotNull
    private static final byte[] SERIALIZED_SUBSCRIBED =
        Base64.getDecoder().decode("CAESMQomCiRhN2NmZDI1Yy03OWJkLTQ4MWMtOTFjYy02OTJlNWRiMWVjM2QRAAAAAAAALkA=");

    @Test
    public void testEncode() {
        assertThat(SERIALIZED_HEARTBEAT).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.encode(HEARTBEAT));
        assertThat(SERIALIZED_OFFER).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.encode(OFFER));
        assertThat(SERIALIZED_SUBSCRIBED).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.encode(SUBSCRIBED));
    }

    @Test
    public void testDecodeSuccess() {
        assertThat(HEARTBEAT).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.decode(SERIALIZED_HEARTBEAT));
        assertThat(OFFER).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.decode(SERIALIZED_OFFER));
        assertThat(SUBSCRIBED).isEqualTo(ProtobufMessageCodecs.SCHEDULER_EVENT.decode(SERIALIZED_SUBSCRIBED));
    }

    @Test
    public void testDecodeFailure() {
        assertDecodeFailure(SERIALIZED_HEARTBEAT);
        assertDecodeFailure(SERIALIZED_OFFER);
        assertDecodeFailure(SERIALIZED_SUBSCRIBED);
    }

    private static void assertDecodeFailure(@NotNull final byte[] protoBytes) {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> ProtobufMessageCodecs.SCHEDULER_CALL.decode(protoBytes))
            .withCauseExactlyInstanceOf(InvalidProtocolBufferException.class);
    }

}
