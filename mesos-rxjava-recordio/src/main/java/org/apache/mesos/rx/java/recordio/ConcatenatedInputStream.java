package org.apache.mesos.rx.java.recordio;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Queue;

final class ConcatenatedInputStream extends FilterInputStream {

    @NotNull
    private static final InputStream EMPTY_STREAM = new ByteArrayInputStream(new byte[0]);

    @NotNull
    private final Queue<InputStream> streams = new ArrayDeque<>();

    public ConcatenatedInputStream() {
        super(EMPTY_STREAM);
    }

    public void append(@NotNull final InputStream inputStream) {
        streams.add(inputStream);
    }

    @Override
    public int read() throws IOException {
        return handleEndOfStream(super::read);
    }

    @Override
    public int read(@NotNull final byte[] b, final int off, final int len) throws IOException {
        return handleEndOfStream(() -> super.read(b, off, len));
    }

    private int handleEndOfStream(final IOIntSupplier method) throws IOException {
        int result;

        while ((result = method.getAsInt()) == -1) {
            in = streams.poll();

            if (in == null) {
                in = EMPTY_STREAM;
                break;
            }
        }

        return result;
    }

    private interface IOIntSupplier {
        int getAsInt() throws IOException;
    }

}
