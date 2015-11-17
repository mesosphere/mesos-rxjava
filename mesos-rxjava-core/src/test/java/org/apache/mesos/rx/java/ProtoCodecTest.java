package org.apache.mesos.rx.java;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.rx.java.test.TestingProtos;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ProtoCodecTest {

    @NotNull
    private static final byte[] SERIALIZED_HEARTBEAT = Base64.getDecoder().decode("CAg=");

    @NotNull
    private static final byte[] SERIALIZED_OFFER = Base64.getDecoder().decode(
        "CAIaagpoCgcKBW9mZmVyEgYKBGZydzEaBwoFc2xhdmUiBWhvc3QxKhYKBGNwdXMQABoJCQA"
            + "AAAAAACBAMgEqKhUKA21lbRAAGgkJAAAAAAAAwEAyASoqFgoEZGlzaxAAGgkJAAAAAAAAwEAyASo="
    );

    @NotNull
    private static final byte[] SERIALIZED_SUBSCRIBED =
        Base64.getDecoder().decode("CAESNQoqCigyMDE1MTAwOC0xNjE0MTctMTY3NzczNDMtNTA1MC0yMDUzMi0wMDA4EQAAAAAAAC5A");

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
