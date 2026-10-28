package com.netpulse.driver;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class NetPulseEngine implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static final int CACHE_LINE_SIZE = 64;
    public static final int DEFAULT_RING_CAPACITY = 65536;
    public static final int DEFAULT_ARENA_SIZE = 1024 * 1024 * 32; // 32MB
    public static final int PACKET_FRAME_SIZE = 256;

    private final MemoryArena memoryArena;
    private final LockFreeRingBuffer ringBuffer;
    private final SlidingWindowFlowControl flowControl;
    private final AtomicLong totalPacketsProcessed;
    private final AtomicLong totalPacketsDropped;
    private volatile boolean running;

    public NetPulseEngine() {
        this(DEFAULT_ARENA_SIZE, DEFAULT_RING_CAPACITY);
    }

    public NetPulseEngine(long arenaCapacityBytes, int ringCapacity) {
        if (Integer.bitCount(ringCapacity) != 1) {
            throw new IllegalArgumentException("Ring capacity must be a power of two: " + ringCapacity);
        }
        this.memoryArena = new MemoryArena(arenaCapacityBytes);
        this.ringBuffer = new LockFreeRingBuffer(ringCapacity);
        this.flowControl = new SlidingWindowFlowControl(1000, 10000);
        this.totalPacketsProcessed = new AtomicLong(0L);
        this.totalPacketsDropped = new AtomicLong(0L);
        this.running = false;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean ingestPacket(ByteBuffer sourceBuffer) {
        if (!running) {
            return false;
        }

        long timestamp = System.nanoTime();
        if (!flowControl.allow(timestamp)) {
            totalPacketsDropped.incrementAndGet();
            return false;
        }

        long address = memoryArena.allocate(PACKET_FRAME_SIZE);
        if (address == 0L) {
            flowControl.signalExhaustion();
            totalPacketsDropped.incrementAndGet();
            return false;
        }

        PacketFrame frame = new PacketFrame(address, PACKET_FRAME_SIZE);
        frame.write(sourceBuffer);

        boolean offered = ringBuffer.offer(frame);
        if (!offered) {
            frame.clear();
            memoryArena.release(address, PACKET_FRAME_SIZE);
            totalPacketsDropped.incrementAndGet();
            return false;
        }

        totalPacketsProcessed.incrementAndGet();
        return true;
    }

    public PacketFrame pollPacket() {
        return ringBuffer.poll();
    }

    public void recyclePacket(PacketFrame frame) {
        if (frame == null) {
            return;
        }
        long address = frame.address();
        int capacity = frame.capacity();
        frame.clear();
        memoryArena.release(address, capacity);
    }

    public long getTotalPacketsProcessed() {
        return totalPacketsProcessed.get();
    }

    public long getTotalPacketsDropped() {
        return totalPacketsDropped.get();
    }

    @Override
    public synchronized void close() {
        stop();
        PacketFrame frame;
        while ((frame = ringBuffer.poll()) != null) {
            frame.clear();
        }
        memoryArena.close();
    }

    public static final class PacketFrame {
        private final long address;
        private final int capacity;

        public PacketFrame(long address, int capacity) {
            this.address = address;
            this.capacity = capacity;
        }

        public long address() {
            return address;
        }

        public int capacity() {
            return capacity;
        }

        public void write(ByteBuffer src) {
            int len = Math.min(src.remaining(), capacity);
            if (src.hasArray()) {
                byte[] arr = src.array();
                int offset = src.arrayOffset() + src.position();
                UNSAFE.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, null, address, len);
                src.position(src.position() + len);
            } else if (src.isDirect()) {
                long srcAddress = ((sun.nio.ch.DirectBuffer) src).address() + src.position();
                UNSAFE.copyMemory(srcAddress, address, len);
                src.position(src.position() + len);
            } else {
                byte[] temp = new byte[len];
                src.get(temp);
                UNSAFE.copyMemory(temp, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, len);
            }
        }

        public void read(ByteBuffer dst) {
            int len = capacity;
            if (dst.hasArray()) {
                byte[] arr = dst.array();
                int offset = dst.arrayOffset() + dst.position();
                UNSAFE.copyMemory(null, address, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, len);
                dst.position(dst.position() + len);
            } else if (dst.isDirect()) {
                long dstAddress = ((sun.nio.ch.DirectBuffer) dst).address() + dst.position();
                UNSAFE.copyMemory(address, dstAddress, len);
                dst.position(dst.position() + len);
            } else {
                byte[] temp = new byte[len];
                UNSAFE.copyMemory(null, address, temp, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
                dst.put(temp);
            }
        }

        public void clear() {
            UNSAFE.setMemory(address, capacity, (byte) 0);
        }
    }

    public static final class LockFreeRingBuffer {
        private final int capacity;
        private final int mask;
        private final Object[] buffer;

        @Contended
        private long head;

        @Contended
        private long tail;

        private static final VarHandle HEAD_HANDLE;
        private static final VarHandle TAIL_HANDLE;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                HEAD_HANDLE = lookup.findVarHandle(LockFreeRingBuffer.class, "head", long.class);
                TAIL_HANDLE = lookup.findVarHandle(LockFreeRingBuffer.class, "tail", long.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public LockFreeRingBuffer(int capacity) {
            if (Integer.bitCount(capacity) != 1) {
                throw new IllegalArgumentException("Capacity must be a power of 2");
            }
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.buffer = new Object[capacity];
            this.head = 0L;
            this.tail = 0L;
        }

        public boolean offer(PacketFrame item) {
            Objects.requireNonNull(item, "Item cannot be null");
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);

            if ((currentTail - currentHead) >= capacity) {
                return false; // Invariant 1: Tail - Head <= Capacity
            }

            if (!TAIL_HANDLE.compareAndSet(this, currentTail, currentTail + 1)) {
                return false;
            }

            int index = (int) (currentTail & mask);
            buffer[index] = item;
            return true;
        }

        public PacketFrame poll() {
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);

            if (currentHead >= currentTail) {
                return null;
            }

            int index = (int) (currentHead & mask);
            Object item = buffer[index];
            if (item == null) {
                return null;
            }

            buffer[index] = null;
            HEAD_HANDLE.setRelease(this, currentHead + 1);
            return (PacketFrame) item;
        }

        public int size() {
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            long size = currentTail - currentHead;
            return size > capacity ? capacity : (int) size;
        }

        public boolean isEmpty() {
            return HEAD_HANDLE.getAcquire(this) == TAIL_HANDLE.getAcquire(this);
        }
    }

    public static final class SlidingWindowFlowControl {
        private final long windowSizeNanos;
        private final int maxPacketsPerWindow;
        private long windowStartNanos;
        private int packetCount;

        public SlidingWindowFlowControl(long windowSizeMillis, int maxPacketsPerWindow) {
            this.windowSizeNanos = windowSizeMillis * 1_000_000L;
            this.maxPacketsPerWindow = maxPacketsPerWindow;
            this.windowStartNanos = System.nanoTime();
            this.packetCount = 0;
        }

        public synchronized boolean allow(long currentNanos) {
            if (currentNanos - windowStartNanos >= windowSizeNanos) {
                windowStartNanos = currentNanos;
                packetCount = 0;
            }
            if (packetCount >= maxPacketsPerWindow) {
                return false;
            }
            packetCount++;
            return true;
        }

        public synchronized void signalExhaustion() {
            // Telemetry hook or internal backpressure state adjustments
        }
    }

    public static final class MemoryArena implements AutoCloseable {
        private final long baseAddress;
        private final long capacity;
        private long offset;
        private boolean closed;

        public MemoryArena(long capacity) {
            this.capacity = capacity;
            this.baseAddress = UNSAFE.allocateMemory(capacity);
            UNSAFE.setMemory(baseAddress, capacity, (byte) 0);
            this.offset = 0L;
            this.closed = false;
        }

        public synchronized long allocate(long size) {
            if (closed) {
                throw new IllegalStateException("MemoryArena is closed");
            }
            long alignedSize = (size + 7) & ~7; // 8-byte alignment
            if (offset + alignedSize > capacity) {
                return 0L; // Invariant 2: Arena allocations strictly bounded
            }
            long allocatedAddress = baseAddress + offset;
            offset += alignedSize;
            return allocatedAddress;
        }

        public synchronized void release(long address, long size) {
            if (closed) {
                return;
            }
            if (address < baseAddress || address >= baseAddress + capacity) {
                throw new IllegalArgumentException("Address outside arena bounds");
            }
            UNSAFE.setMemory(address, size, (byte) 0); // Invariant 2: Zeroized metadata
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                UNSAFE.freeMemory(baseAddress);
            }
        }

        public long capacity() {
            return capacity;
        }

        public synchronized long usedMemory() {
            return offset;
        }
    }

    @java.lang.annotation.Target({java.lang.annotation.ElementType.FIELD})
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    public @interface Contended {
    }
}