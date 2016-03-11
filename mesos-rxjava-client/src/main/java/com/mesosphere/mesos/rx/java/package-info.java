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
/**
 * This package contains defines a client used to interact with a Mesos HTTP API such as the
 * <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md" target="_blank">Scheduler HTTP API</a>.
 *
 * <h2>Design</h2>
 * This library is designed to allow a user to interact with Mesos by defining a function that receives a stream
 * ({@link rx.Observable Observable}) of
 * <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md#events" target="_blank">Events</a>
 * from Mesos potentially emitting events of it's own that will be sent to Mesos.
 * <p>
 * However this function is implemented doesn't matter to the library, as long as the function contract is upheld.
 * <p>
 * This module is ignorant of the actual messages being sent and received from Mesos and the serialization mechanism
 * used. Instead, the user provides a {@link com.mesosphere.mesos.rx.java.util.MessageCodec MessageCodec} for each
 * of the corresponding messages.  The advantage of this is that the client can stay buffered from message changes
 * made my Mesos as well as serialization used (the package
 * <a href="protobuf/package-summary.html">com.mesosphere.mesos.rx.java.protobuf</a> provides the codecs necessary to use
 * protobuf when connecting to mesos).
 */
package com.mesosphere.mesos.rx.java;
