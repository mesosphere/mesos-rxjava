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

package com.mesosphere.mesos.rx.java.test;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A utility class that can used to create a proxy between Mesos and your client,
 * and then close the connection to verify behavior.
 */
public final class TcpSocketProxy implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpSocketProxy.class);

    @NotNull
    private final AtomicInteger threadCounter = new AtomicInteger(1);
    @NotNull
    private final ExecutorService service = Executors.newCachedThreadPool(
        r -> new Thread(r, "TcpSocketProxy-" + threadCounter.getAndIncrement())
    );

    @NotNull
    private final ScheduledExecutorService murderService = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "TcpSocketProxy-murderer")
    );

    @NotNull
    private final InetSocketAddress listen;
    @NotNull
    private final InetSocketAddress dest;

    @NotNull
    private final List<Socket> sockets;

    @NotNull
    private final AtomicReference<ServerSocket> socketRef;

    @NotNull
    private final List<Future<?>> futures;
    @NotNull
    private final AtomicBoolean shutdown;

    @NotNull
    private final CountDownLatch startupCountDownLatch;
    @NotNull
    private final CountDownLatch shutdownCountDownLatch;

    @Nullable
    private ScheduledFuture<?> murderer;

    /**
     * Define a proxy between two addresses.  All bytes sent to {@code listen} will be forwarded to {@code destination}
     * and vice versa.  Due to this proxy being "dumb" (raw sockets listening and opened to destination) socket level
     * smarts like TLS are not necessarily supported.
     * @param listen         The {@link InetSocketAddress} the proxy should listen on.
     * @param destination    The {@link InetSocketAddress} the proxy should open a socket to.
     */
    public TcpSocketProxy(@NotNull final InetSocketAddress listen, @NotNull final InetSocketAddress destination) {
        this.listen = listen;
        this.dest = destination;

        sockets = Collections.synchronizedList(new ArrayList<>());
        socketRef = new AtomicReference<>();
        futures = Collections.synchronizedList(new ArrayList<>());
        shutdown = new AtomicBoolean(false);
        startupCountDownLatch = new CountDownLatch(1);
        shutdownCountDownLatch = new CountDownLatch(1);
    }

    /**
     * Start the proxy in the background.
     */
    public void start() {
        if (shutdown.get()) {
            throw new IllegalStateException("Can not start an already shutdown proxy");
        }
        final Future<?> submit = service.submit(new ProxyServer());

        futures.add(submit);
    }

    /**
     * Get the port the proxy is listening on. This method will block until the proxy is up and running, meaning
     * {@link #start()} will need to be called before this method can return a value.
     * @return The port the proxy is listening on.
     */
    public int getListenPort() {
        try {
            startupCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return socketRef.get().getLocalPort();
    }

    /**
     * Returns a boolean indicating whether the proxy has been shutdown or not.
     * @return a boolean indicating whether the proxy has been shutdown or not.
     */
    public boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * Useful wrapper method allowing the use of a proxy in a try-with block.
     * <p>
     * Calls {@link #shutdown()} and tears down all background threads.
     * @see #shutdown()
     */
    @Override
    public void close() {
        shutdown();
        if (!murderService.isShutdown()) {
            murderService.shutdownNow();
        }
        murderer.cancel(true);
        murderer = null;
    }

    /**
     * Immediately run the shutdown process for this proxy.
     * <p>
     * Shutting down the proxy will result in all open sockets being closed and the listening socket being closed.
     * <p>
     * This method will block until shutdown has completed.
     *
     * @see #shutdownIn(long, TimeUnit)
     */
    public void shutdown() {
        if (!service.isTerminated()) {
            shutdownIn(0, TimeUnit.SECONDS);
        }
        try {
            shutdownCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run the shutdown process for this proxy in {@code delay} {@code timeUnit}.
     * @param delay       The time from now to when shutdown should begin
     * @param timeUnit    The time unit of the delay parameter
     * @see ScheduledExecutorService#schedule(Callable, long, TimeUnit)
     */
    public void shutdownIn(final long delay, @NotNull final TimeUnit timeUnit) {
        this.murderer = murderService.schedule(new ServerShutdownTask(), delay, timeUnit);
    }

    private final class ProxyServer implements Runnable {
        @Override
        public void run() {
            try {
                final ServerSocket socket = new ServerSocket(listen.getPort());
                final String proxyDescription = String.format(
                    "%s:%d -> %s:%d",
                    socket.getInetAddress(),
                    socket.getLocalPort(),
                    dest.getAddress(),
                    dest.getPort()
                );
                LOGGER.info("Creating proxy: {}", proxyDescription);
                socketRef.set(socket);
                startupCountDownLatch.countDown();
                while (true) {
                    final Socket accept = socket.accept();
                    final InputStream in = accept.getInputStream();
                    final OutputStream out = accept.getOutputStream();

                    final Socket downStream = new Socket(dest.getAddress(), dest.getPort());
                    final OutputStream send = downStream.getOutputStream();
                    final InputStream ret = downStream.getInputStream();
                    final Future<?> f1 = service.submit(new ByteCopyRunnable(in, send));
                    final Future<?> f2 = service.submit(new ByteCopyRunnable(ret, out));
                    sockets.add(accept);
                    sockets.add(downStream);
                    futures.add(f1);
                    futures.add(f2);
                }
            } catch (SocketException se) {
                switch (se.getMessage()) {
                    case "Socket closed":
                        break;
                    default:
                        final String proxyDescription = String.format(
                            "%s:%d -> %s:%d",
                            listen.getAddress(),
                            listen.getPort(),
                            dest.getAddress(),
                            dest.getPort()
                        );
                        LOGGER.error(String.format("Error creating proxy: %s", proxyDescription), se);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class ServerShutdownTask implements Runnable {
        @Override
        public void run() {
            LOGGER.debug("Proxy shutdown starting...");
            shutdown.set(true);
            try {
                if (!service.isShutdown()) {
                    service.shutdown();
                }
                for (Future<?> f : futures) {
                    f.cancel(true);
                }
                final ServerSocket serverSocket = socketRef.get();
                if (!serverSocket.isClosed()) {
                    serverSocket.close();
                }
                for (Socket socket : sockets) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                        // if we get an exception trying to close one socket swallow it and move onto the next one
                    }
                }
                LOGGER.debug("Proxy shutdown complete");
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                shutdownCountDownLatch.countDown();
            }
        }
    }

    private static final class ByteCopyRunnable implements Runnable {
        @NotNull
        private final InputStream in;
        @NotNull
        private final OutputStream out;

        public ByteCopyRunnable(@NotNull final InputStream in, @NotNull final OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                final byte[] buffer = new byte[0x1000]; // 4k
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
