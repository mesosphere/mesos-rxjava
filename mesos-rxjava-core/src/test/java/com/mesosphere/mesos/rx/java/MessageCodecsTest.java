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

import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.mesos.rx.java.test.TestingProtos;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public final class MessageCodecsTest {

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
        assertThat(SERIALIZED_HEARTBEAT).isEqualTo(MessageCodecs.PROTOS_EVENT.encode(TestingProtos.HEARTBEAT));
        assertThat(SERIALIZED_OFFER).isEqualTo(MessageCodecs.PROTOS_EVENT.encode(TestingProtos.OFFER));
        assertThat(SERIALIZED_SUBSCRIBED).isEqualTo(MessageCodecs.PROTOS_EVENT.encode(TestingProtos.SUBSCRIBED));
    }

    @Test
    public void testDecodeSuccess() {
        assertThat(TestingProtos.HEARTBEAT).isEqualTo(MessageCodecs.PROTOS_EVENT.decode(SERIALIZED_HEARTBEAT));
        assertThat(TestingProtos.OFFER).isEqualTo(MessageCodecs.PROTOS_EVENT.decode(SERIALIZED_OFFER));
        assertThat(TestingProtos.SUBSCRIBED).isEqualTo(MessageCodecs.PROTOS_EVENT.decode(SERIALIZED_SUBSCRIBED));
    }

    @Test
    public void testDecodeFailure() {
        assertDecodeFailure(SERIALIZED_HEARTBEAT);
        assertDecodeFailure(SERIALIZED_OFFER);
        assertDecodeFailure(SERIALIZED_SUBSCRIBED);
    }

    private static void assertDecodeFailure(@NotNull final byte[] protoBytes) {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> MessageCodecs.PROTOS_CALL.decode(protoBytes))
            .withCauseExactlyInstanceOf(InvalidProtocolBufferException.class);
    }

}
