package org.apache.mesos.rx.java.test;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
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

    @Nullable
    private ScheduledFuture<?> murderer;

    public TcpSocketProxy(@NotNull final InetSocketAddress listen, @NotNull final InetSocketAddress destination) {
        this.listen = listen;
        this.dest = destination;

        sockets = Lists.newCopyOnWriteArrayList();
        socketRef = new AtomicReference<>();
        futures = Lists.newCopyOnWriteArrayList();
        shutdown = new AtomicBoolean(false);
    }

    public void run() throws IOException {
        if (shutdown.get()) {
            throw new IllegalStateException("Can not run an already shutdown proxy");
        }
        final Future<?> submit = service.submit(new ProxyServer());

        futures.add(submit);
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public void close() throws IOException {
        shutdown();
        if (!murderService.isShutdown()) {
            murderService.shutdownNow();
        }
        murderer.cancel(true);
        murderer = null;
    }

    public void shutdown() {
        if (!service.isTerminated()) {
            shutdownIn(0, TimeUnit.SECONDS);
        }
    }

    public void shutdownIn(final long delay, @NotNull final TimeUnit timeUnit) {
        this.murderer = murderService.schedule(new ServerShutdownTask(), delay, timeUnit);
    }

    private final class ProxyServer implements Runnable {
        @Override
        public void run() {
            final String proxyDescription = String.format(
                "%s:%d -> %s:%d",
                listen.getAddress(),
                listen.getPort(),
                dest.getAddress(),
                dest.getPort());
            try {
                LOGGER.info("Creating proxy: {}", proxyDescription);
                final ServerSocket socket = new ServerSocket(listen.getPort());
                socketRef.set(socket);
                while (true) {
                    final Socket accept = socket.accept();
                    final InputStream in = accept.getInputStream();
                    final OutputStream out = accept.getOutputStream();

                    final Socket mesos = new Socket(dest.getAddress(), dest.getPort());
                    final OutputStream send = mesos.getOutputStream();
                    final InputStream ret = mesos.getInputStream();
                    final Future<?> f1 = service.submit(() -> {
                        try {
                            ByteStreams.copy(in, send);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    final Future<?> f2 = service.submit(() -> {
                        try {
                            ByteStreams.copy(ret, out);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    sockets.add(accept);
                    sockets.add(mesos);
                    futures.add(f1);
                    futures.add(f2);
                }
            } catch (SocketException se) {
                switch (se.getMessage()) {
                    case "Socket closed":
                        break;
                    default:
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
            }
        }
    }
}
