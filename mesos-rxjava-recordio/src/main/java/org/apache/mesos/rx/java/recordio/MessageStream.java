package org.apache.mesos.rx.java.recordio;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

final class MessageStream {

    @NotNull
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStream.class);

    /**
     * Maximum number of base-10 digits in a uint64
     */
    private static final int MESSAGE_SIZE_MAX_LENGTH = 20;

    @NotNull
    private final InputStream source;

    private int messageSizeBytesRead = 0;

    /**
     * The message size from the stream is provided as a base-10 String representation of an
     * unsigned 64 bit integer (uint64). Since there is a possibility (since the spec isn't
     * formal on this, and the HTTP chunked Transfer-Encoding applied to the data stream can
     * allow chunks to be any size) this field functions as the bytes that have been read
     * since the end of the last message. When the next '\n' is encountered in the byte
     * stream, these bytes are converted to a UTF-8 String. This string representation is
     * then read as a {@code long} using {@link Long#valueOf(String, int)}.
     */
    @NotNull
    private final byte[] messageSizeBytes = new byte[MESSAGE_SIZE_MAX_LENGTH];

    /**
     * The number of bytes in the encoding is specified as an unsigned (uint64)
     * However, since arrays in java are addressed and indexed by int we drop the
     * precision early so that working with the arrays is easier.
     * Also, a byte[Integer.MAX_VALUE] is 2gb so I seriously doubt we'll be receiving
     * a message that large.
     */
    private int remainingBytesForMessage = 0;

    /**
     * The allocated {@code byte[]} for the current message being read from the stream.
     * It is null until the first message is read, and is reassigned for each new message.
     */
    @Nullable
    private byte[] messageBytes;


    public MessageStream(@NotNull final InputStream source) {
        this.source = source;
    }

    /**
     * Gets the next message from this stream, if possible.
     *
     * @return the next complete message from the front of this stream, or {@code null} if not enough data is
     * available.
     * @throws IOException if an error occurs when reading data from the appended {@link ByteBuf}s.
     */
    @Nullable
    public byte[] next() throws IOException {
        if (remainingBytesForMessage == 0 || messageBytes == null) {
            long messageSize = nextMessageSize();

            if (messageSize < 0) {
                return null;
            }

            if (messageSize > Integer.MAX_VALUE) {
                // TODO: Possibly make this more robust to account for things larger than 2g
                String error = "Specified message size " + messageSize + " is larger than Integer.MAX_VALUE";
                throw new IllegalStateException(error);
            }

            remainingBytesForMessage = (int) messageSize;
            messageBytes = new byte[remainingBytesForMessage];
        }

        final int startIndex = messageBytes.length - remainingBytesForMessage;
        final int bytesRead = source.read(messageBytes, startIndex, remainingBytesForMessage);
        remainingBytesForMessage -= Math.max(bytesRead, 0);

        return (remainingBytesForMessage == 0) ? messageBytes : null;
    }


    private long nextMessageSize() throws IOException {
        while (true) {
            int i = source.read();
            if (i == -1) {
                return -1L;
            }

            byte b = (byte) i;
            if (b == (byte) '\n') {
                break;
            }

            try {
                messageSizeBytes[messageSizeBytesRead] = b;
            } catch (ArrayIndexOutOfBoundsException e) {
                String error = "Message size field exceeds limit of " + messageSizeBytes.length + " bytes";
                throw new IllegalStateException(error, e);
            }

            messageSizeBytesRead++;
        }

        final String messageSizeString = new String(messageSizeBytes, 0, messageSizeBytesRead, Charsets.UTF_8);
        messageSizeBytesRead = 0;

        return Long.valueOf(messageSizeString);
    }

}
