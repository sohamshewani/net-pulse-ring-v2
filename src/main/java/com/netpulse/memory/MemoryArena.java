package com.netpulse.memory;

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class MemoryArena implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final long FREE_LIST_NEXT_OFFSET = 0L;
    private static final long BLOCK_HEADER_SIZE = 64;

    private final long totalCapacity;
    private final long baseAddress;
    private final Arena foreignArena;
    private final MemorySegment memorySegment;

    @SuppressWarnings("unused")
    private volatile long bumpPointer;
    @SuppressWarnings("unused")
    private volatile long freeListHead;

    private static final VarHandle BUMP_POINTER_HANDLE;
    private static final VarHandle FREE_LIST_HEAD_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            BUMP_POINTER_HANDLE = l.findVarHandle(MemoryArena.class, "bumpPointer", long.class);
            FREE_LIST_HEAD_HANDLE = l.findVarHandle(MemoryArena.class, "freeListHead", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final AtomicLong allocatedBytes = new AtomicLong(0L);
    private final AtomicLong activeAllocations = new AtomicLong(0L);
    private final AtomicLong exhaustionCount = new AtomicLong(0L);
    private final AtomicReference<ArenaState> state = new AtomicReference<>(ArenaState.ACTIVE);

    public enum ArenaState {
        ACTIVE,
        EXHAUSTED,
        CLOSED
    }

    public MemoryArena(long capacityBytes) {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive");
        }
        
        long remainder = capacityBytes % CACHE_LINE_SIZE;
        if (remainder != 0) {
            capacityBytes += (CACHE_LINE_SIZE - remainder);
        }
        
        this.totalCapacity = capacityBytes;
        this.foreignArena = Arena.ofShared();
        this.memorySegment = foreignArena.allocate(capacityBytes, CACHE_LINE_SIZE);
        this.baseAddress = memorySegment.address();
        
        this.bumpPointer = baseAddress;
        this.freeListHead = 0L;
    }

    public long allocate(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Allocation size must be positive");
        }

        long alignedSize = alignSize(size + BLOCK_HEADER_SIZE);

        if (state.get() == ArenaState.CLOSED) {
            throw new IllegalStateException("MemoryArena is closed");
        }

        while (true) {
            long currentFreeHead = (long) FREE_LIST_HEAD_HANDLE.getAcquire(this);
            if (currentFreeHead != 0L) {
                long nextFree = UNSAFE.getAddress(currentFreeHead + FREE_LIST_NEXT_OFFSET);
                if (FREE_LIST_HEAD_HANDLE.compareAndSet(this, currentFreeHead, nextFree)) {
                    long userAddress = currentFreeHead + BLOCK_HEADER_SIZE;
                    UNSAFE.putLong(currentFreeHead, alignedSize);
                    allocatedBytes.addAndGet(alignedSize);
                    activeAllocations.incrementAndGet();
                    return userAddress;
                }
                continue;
            }
            break;
        }

        while (true) {
            long currentBump = (long) BUMP_POINTER_HANDLE.getAcquire(this);
            long nextBump = currentBump + alignedSize;
            
            if (nextBump > baseAddress + totalCapacity) {
                state.compareAndSet(ArenaState.ACTIVE, ArenaState.EXHAUSTED);
                exhaustionCount.incrementAndGet();
                return 0L;
            }

            if (BUMP_POINTER_HANDLE.compareAndSet(this, currentBump, nextBump)) {
                long userAddress = currentBump + BLOCK_HEADER_SIZE;
                UNSAFE.putLong(currentBump, alignedSize);
                allocatedBytes.addAndGet(alignedSize);
                activeAllocations.incrementAndGet();
                return userAddress;
            }
        }
    }

    public void free(long userAddress) {
        if (userAddress == 0L) {
            return;
        }

        long blockHeaderAddress = userAddress - BLOCK_HEADER_SIZE;
        if (blockHeaderAddress < baseAddress || blockHeaderAddress >= baseAddress + totalCapacity) {
            throw new IllegalArgumentException("Address out of arena bounds: " + userAddress);
        }

        long blockSize = UNSAFE.getLong(blockHeaderAddress);
        
        UNSAFE.setMemory(blockHeaderAddress, blockSize, (byte) 0);

        while (true) {
            long currentHead = (long) FREE_LIST_HEAD_HANDLE.getAcquire(this);
            UNSAFE.putAddress(blockHeaderAddress + FREE_LIST_NEXT_OFFSET, currentHead);
            
            if (FREE_LIST_HEAD_HANDLE.compareAndSet(this, currentHead, blockHeaderAddress)) {
                allocatedBytes.addAndGet(-blockSize);
                activeAllocations.decrementAndGet();
                state.compareAndSet(ArenaState.EXHAUSTED, ArenaState.ACTIVE);
                break;
            }
        }
    }

    public ByteBuffer sliceDirectBuffer(long userAddress, int length) {
        if (userAddress == 0L) {
            throw new IllegalArgumentException("Cannot slice null address");
        }
        long blockHeaderAddress = userAddress - BLOCK_HEADER_SIZE;
        long blockSize = UNSAFE.getLong(blockHeaderAddress);
        long maxUserSize = blockSize - BLOCK_HEADER_SIZE;

        if (length < 0 || length > maxUserSize) {
            throw new IllegalArgumentException("Requested length " + length + " exceeds block capacity " + maxUserSize);
        }

        MemorySegment slice = memorySegment.asSlice(userAddress - baseAddress, length);
        ByteBuffer buffer = slice.asByteBuffer();
        return buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    private long alignSize(long size) {
        long remainder = size % CACHE_LINE_SIZE;
        if (remainder == 0) {
            return size;
        }
        return size + (CACHE_LINE_SIZE - remainder);
    }

    public ArenaState getState() {
        return state.get();
    }

    public long getAllocatedBytes() {
        return allocatedBytes.get();
    }

    public long getActiveAllocations() {
        return activeAllocations.get();
    }

    public long getExhaustionCount() {
        return exhaustionCount.get();
    }

    public long getTotalCapacity() {
        return totalCapacity;
    }

    @Override
    public void close() {
        if (state.getAndSet(ArenaState.CLOSED) != ArenaState.CLOSED) {
            foreignArena.close();
        }
    }
}