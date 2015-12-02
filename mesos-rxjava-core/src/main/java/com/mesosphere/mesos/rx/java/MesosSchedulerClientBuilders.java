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

import org.apache.mesos.v1.scheduler.Protos;
import org.jetbrains.annotations.NotNull;

/**
 * A collection of methods that have some pre-defined {@link MesosSchedulerClientBuilder} configurations.
 */
public final class MesosSchedulerClientBuilders {
    /**
     * @return  An initial {@link MesosSchedulerClientBuilder} that will use protobuf
     *          for the {@link org.apache.mesos.v1.scheduler.Protos.Call} and
     *          {@link org.apache.mesos.v1.scheduler.Protos.Event} messages.
     */
    @NotNull
    public static MesosSchedulerClientBuilder<Protos.Call, Protos.Event> usingProtos() {
        return MesosSchedulerClientBuilder.<Protos.Call, Protos.Event>newBuilder()
            .sendCodec(MessageCodecs.PROTOS_CALL)
            .receiveCodec(MessageCodecs.PROTOS_EVENT)
            ;
    }
}
