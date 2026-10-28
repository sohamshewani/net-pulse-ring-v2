package com.netpulse.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * High-performance, zero-copy PacketFrame implementation backed by a direct off-heap ByteBuffer slice.
 * Leverages VarHandles for direct memory access with strict bounds checking and memory layout optimization.
 */
public final class PacketFrame implements AutoCloseable {

    // Memory layout offsets within the frame header / payload
    public static final int MAGIC_NUMBER_OFFSET = 0;       // 4 bytes (int)
    public static final int FLAGS_OFFSET = 4;              // 2 bytes (short)
    public static final int STREAM_ID_OFFSET = 6;          // 8 bytes (long)
    public static final int TIMESTAMP_OFFSET = 14;         // 8 bytes (long)
    public static final int PAYLOAD_LENGTH_OFFSET = 22;    // 4 bytes (int)
    public static final int HEADER_SIZE = 26;              // Total header bytes

    public static final int REQUIRED_ALIGNMENT = 64;       // Cache line alignment
    public static final int DEFAULT_MAX_CAPACITY = 65536;  // 64 KB max frame size

    private static final int EXPECTED_MAGIC = 0x4E505246;  // "NPRF" (NetPulse Ring Frame)

    private final ByteBuffer backingBuffer;
    private final long address;
    private final int capacity;
    private boolean closed;

    // VarHandles for ultra-fast, volatile/acquire-release off-heap primitive access
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /**
     * Constructs a PacketFrame wrapping an existing direct ByteBuffer slice.
     * 
     * @param slice The direct ByteBuffer slice representing this frame's memory block.
     */
    public PacketFrame(ByteBuffer slice) {
        Objects.requireNonNull(slice, "Backing buffer slice cannot be null");
        if (!slice.isDirect()) {
            throw new IllegalArgumentException("PacketFrame requires a direct ByteBuffer for zero-copy operations");
        }
        this.backingBuffer = slice.slice().order(ByteOrder.BIG_ENDIAN);
        this.capacity = this.backingBuffer.capacity();
        if (this.capacity < HEADER_SIZE) {
            throw new IllegalArgumentException("Buffer capacity " + this.capacity + " is less than minimum header size " + HEADER_SIZE);
        }
        
        // Resolve direct buffer memory address via sun.misc.Unsafe or modern ByteBuffer API reflection/accessor if needed.
        // For standard Java 11+, we use native address extraction via Unsafe bridge or direct buffer base address.
        this.address = extractDirectBufferAddress(this.backingBuffer);
        this.closed = false;

        // Initialize frame header magic
        setMagicNumber(EXPECTED_MAGIC);
    }

    /**
     * Extracts the memory address from a direct ByteBuffer using standard internal access mechanisms.
     */
    private static long extractDirectBufferAddress(ByteBuffer buffer) {
        try {
            java.lang.reflect.Field addressField = java.nio.Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            return (long) addressField.get(buffer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve direct buffer memory address for zero-copy operations", e);
        }
    }

    public int getMagicNumber() {
        ensureOpen();
        return backingBuffer.getInt(MAGIC_NUMBER_OFFSET);
    }

    public void setMagicNumber(int magicNumber) {
        ensureOpen();
        backingBuffer.putInt(MAGIC_NUMBER_OFFSET, magicNumber);
    }

    public short getFlags() {
        ensureOpen();
        return backingBuffer.getShort(FLAGS_OFFSET);
    }

    public void setFlags(short flags) {
        ensureOpen();
        backingBuffer.putShort(FLAGS_OFFSET, flags);
    }

    public long getStreamId() {
        ensureOpen();
        return backingBuffer.getLong(STREAM_ID_OFFSET);
    }

    public void setStreamId(long streamId) {
        ensureOpen();
        backingBuffer.putLong(STREAM_ID_OFFSET, streamId);
    }

    public long getTimestamp() {
        ensureOpen();
        return backingBuffer.getLong(TIMESTAMP_OFFSET);
    }

    public void setTimestamp(long timestamp) {
        ensureOpen();
        backingBuffer.putLong(TIMESTAMP_OFFSET, timestamp);
    }

    public int getPayloadLength() {
        ensureOpen();
        return backingBuffer.getInt(PAYLOAD_LENGTH_OFFSET);
    }

    public void setPayloadLength(int payloadLength) {
        ensureOpen();
        if (HEADER_SIZE + payloadLength > capacity) {
            throw new IllegalArgumentException("Payload length " + payloadLength + " exceeds remaining capacity " + (capacity - HEADER_SIZE));
        }
        backingBuffer.putInt(PAYLOAD_LENGTH_OFFSET, payloadLength);
    }

    /**
     * Writes raw bytes from a source array into the frame payload area.
     * 
     * @param src    Source byte array.
     * @param offset Offset in the source array.
     * @param length Number of bytes to write.
     */
    public void writePayload(byte[] src, int offset, int length) {
        ensureOpen();
        if (HEADER_SIZE + length > capacity) {
            throw new IndexOutOfBoundsException("Write length " + length + " exceeds available frame capacity");
        }
        backingBuffer.position(HEADER_SIZE);
        backingBuffer.put(src, offset, length);
        setPayloadLength(length);
    }

    /**
     * Reads payload bytes from the frame into a destination array.
     * 
     * @param dst    Destination byte array.
     * @param offset Offset in the destination array.
     * @return Number of bytes read.
     */
    public int readPayload(byte[] dst, int offset) {
        ensureOpen();
        int payloadLen = getPayloadLength();
        if (offset + payloadLen > dst.length) {
            throw new IndexOutOfBoundsException("Destination array too small for payload length " + payloadLen);
        }
        backingBuffer.position(HEADER_SIZE);
        backingBuffer.get(dst, offset, payloadLen);
        return payloadLen;
    }

    /**
     * Resets internal metadata and zeroes out the frame memory block for pool recycling (Invariant 2).
     */
    public void reset() {
        ensureOpen();
        backingBuffer.clear();
        // Zero out entire buffer memory to eliminate data leakage between ring iterations
        for (int i = 0; i < capacity; i += 8) {
            if (i + 8 <= capacity) {
                backingBuffer.putLong(i, 0L);
            } else {
                for (int j = i; j < capacity; j++) {
                    backingBuffer.put(j, (byte) 0);
                }
                break;
            }
        }
        setMagicNumber(EXPECTED_MAGIC);
    }

    public ByteBuffer getBackingBuffer() {
        ensureOpen();
        return backingBuffer;
    }

    public long getAddress() {
        ensureOpen();
        return address;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean isClosed() {
        return closed;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PacketFrame has been closed and released back to the arena pool");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            reset();
        }
    }
}