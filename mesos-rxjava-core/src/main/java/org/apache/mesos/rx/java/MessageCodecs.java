package org.apache.mesos.rx.java;

import org.apache.mesos.v1.scheduler.Protos;

/**
 * Utilities for working with {@link MessageCodec} instances.
 */
public final class MessageCodecs {

    /** A {@link org.apache.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Call}. */
    public static final MessageCodec<Protos.Call> PROTOS_CALL = new ProtoCodec<>(Protos.Call::parseFrom);

    /** A {@link org.apache.mesos.rx.java.MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Event}. */
    public static final MessageCodec<Protos.Event> PROTOS_EVENT = new ProtoCodec<>(Protos.Event::parseFrom);

    private MessageCodecs() {}

}
