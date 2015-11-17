package org.apache.mesos.rx.java;

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
