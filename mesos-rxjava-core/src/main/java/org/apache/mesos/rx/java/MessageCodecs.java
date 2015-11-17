package org.apache.mesos.rx.java;

import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

/**
 * Utilities for working with {@link MessageCodec} instances.
 */
public final class MessageCodecs {

    /** A {@link org.apache.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Call}. */
    public static final MessageCodec<Protos.Call> PROTOS_CALL = new ProtoCodec<>(Protos.Call::parseFrom);

    /** A {@link org.apache.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Event}. */
    public static final MessageCodec<Protos.Event> PROTOS_EVENT = new ProtoCodec<>(Protos.Event::parseFrom);

    @NotNull
    public static final MessageCodec<String> UTF8_STRING = new StringMessageCodec();

    private MessageCodecs() {}

    /**
     * UTF_8 String codec
     * @see StandardCharsets#UTF_8
     */
    private static final class StringMessageCodec implements MessageCodec<String> {
        @NotNull
        @Override
        public byte[] encode(@NotNull final String message) {
            return message.getBytes(StandardCharsets.UTF_8);
        }

        @NotNull
        @Override
        public String decode(@NotNull final byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @NotNull
        @Override
        public String mediaType() {
            return "text/plain";
        }

        @NotNull
        @Override
        public String show(@NotNull final String message) {
            return message;
        }
    }

}
