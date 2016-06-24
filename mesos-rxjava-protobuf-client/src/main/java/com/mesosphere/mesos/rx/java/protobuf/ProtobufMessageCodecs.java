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

import com.mesosphere.mesos.rx.java.util.MessageCodec;
import org.apache.mesos.v1.scheduler.Protos;

/**
 * A pair of {@link MessageCodec}s for {@link org.apache.mesos.v1.scheduler.Protos.Call} and
 * {@link org.apache.mesos.v1.scheduler.Protos.Event}
 */
public final class ProtobufMessageCodecs {

    private ProtobufMessageCodecs() {}

    /** A {@link MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Call Call}. */
    public static final MessageCodec<Protos.Call> SCHEDULER_CALL = new ProtoCodec<>(
        Protos.Call::parseFrom, Protos.Call::parseFrom
    );

    /** A {@link MessageCodec} for {@link org.apache.mesos.v1.scheduler.Protos.Event Event}. */
    public static final MessageCodec<Protos.Event> SCHEDULER_EVENT = new ProtoCodec<>(
        Protos.Event::parseFrom, Protos.Event::parseFrom
    );

    /** A {@link MessageCodec} for {@link org.apache.mesos.v1.executor.Protos.Call Call}. */
    public static final MessageCodec<org.apache.mesos.v1.executor.Protos.Call> EXECUTOR_CALL = new ProtoCodec<>(
        org.apache.mesos.v1.executor.Protos.Call::parseFrom, org.apache.mesos.v1.executor.Protos.Call::parseFrom
    );

    /** A {@link MessageCodec} for {@link org.apache.mesos.v1.executor.Protos.Event Event}. */
    public static final MessageCodec<org.apache.mesos.v1.executor.Protos.Event> EXECUTOR_EVENT = new ProtoCodec<>(
        org.apache.mesos.v1.executor.Protos.Event::parseFrom, org.apache.mesos.v1.executor.Protos.Event::parseFrom
    );

}
