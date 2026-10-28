package com.netpulse.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer
 * designed for zero-copy off-heap packet processing in net-pulse-ring.
 * 
 * Implements cache-line padding to eliminate false sharing between producer
 * and consumer threads, and utilizes power-of-two capacity sizing for fast
 * bitwise modulo operations.
 */
public final class LockFreeRingBuffer<E> {

    private final int capacity;
    private final int mask;
    private final Object[] buffer;

    // Cache line padding to prevent false sharing
    // 64 bytes = 8 longs / references
    
    // Padding before head
    private long p1, p2, p3, p4, p5, p6, p7;
    
    // Head sequence (consumer read pointer)
    private volatile long head;
    
    // Padding between head and tail
    private long p8, p9, p10, p11, p12, p13, p14, p15;
    
    // Tail sequence (producer write pointer)
    private volatile long tail;
    
    // Padding after tail
    private long p16, p17, p18, p19, p20, p21, p22;

    private static final VarHandle HEAD_HANDLE;
    private static final VarHandle TAIL_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "head", long.class);
            TAIL_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final AtomicLong overflowCounter = new AtomicLong(0L);

    /**
     * Constructs a new LockFreeRingBuffer with the specified capacity.
     * Capacity is automatically rounded up to the nearest power of two.
     *
     * @-param requestedCapacity the desired capacity of the ring buffer
     */
    public LockFreeRingBuffer(int requestedCapacity) {
        if (requestedCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive.");
        }
        this.capacity = ceilingNextPowerOfTwo(requestedCapacity);
        this.mask = this.capacity - 1;
        this.buffer = new Object[this.capacity];
        
        // Initialize sequence tracking arrays or markers if required, 
        // but for MPMC we use individual slot sequence states or standard ticket locking.
        // To ensure strict lock-free MPMC without locks per slot, we implement 
        // the classic Dmitry Vyukov bounded MPMC queue algorithm.
    }

    private static int ceilingNextPowerOfTwo(int x) {
        return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
    }

    /**
     * Returns the capacity of the ring buffer.
     *
     * @return capacity rounded to power of two
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Retrieves the total number of dropped elements due to ring buffer overflows.
     *
     * @return cumulative drop count
     */
    public long getOverflowCount() {
        return overflowCounter.get();
    }

    /**
     * Attempts to push an element into the ring buffer.
     * Non-blocking, thread-safe for multi-producer environments.
     *
     * @-param e the element to push (non-null)
     * @return true if successfully pushed, false if the buffer is full
     */
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException("Null elements are not permitted in LockFreeRingBuffer.");
        }

        long currentTail;
        long currentHead;
        
        // Vyukov MPMC bounded queue algorithm implementation via VarHandle CAS loops
        // Each slot in the buffer can conceptually track its generation sequence, 
        // but for simplicity and raw throughput in Java, we use atomic sequence arrays 
        // or a robust ticket-based CAS approach. Let's use the sequence-based slot array 
        // for absolute MPMC correctness.
        
        // Wait, to keep this self-contained and fully implemented within a single class file,
        // let's embed the sequence tracking via an auxiliary parallel long array.
        return offerInternal(e);
    }

    // Auxiliary sequence array for MPMC coordination
    private final long[] sequence;
    {
        // Initialized via instance initializer block matching capacity
        // Note: capacity is computed dynamically, so we initialize sequence in constructor.
    }

    // Overloading constructor logic to properly initialize sequence array:
    /*
     * Re-declaring fields and constructor correctly:
     */
    
    // We will place the full implementation with sequence array inside a cohesive block.
    // Let's rewrite the class body cleanly with the sequence array fully integrated.
}

package com.netpulse.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer
 * utilizing Dmitry Vyukov's bounded queue algorithm with cache-line padding
 * and acquire-release memory semantics via VarHandles.
 */
public final class LockFreeRingBuffer<E> {

    private final int capacity;
    private final int mask;
    private final Object[] buffer;
    private final long[] sequence;

    // Cache-line padding for head pointer
    private long p01, p02, p03, p04, p05, p06, p07;
    private volatile long head;
    private long p08, p09, p10, p11, p12, p13, p14;

    // Cache-line padding for tail pointer
    private long p15, p16, p17, p18, p19, p20, p21;
    private volatile long tail;
    private long p22, p23, p24, p25, p26, p27, p28;

    private static final VarHandle HEAD_HANDLE;
    private static final VarHandle TAIL_HANDLE;

    private final AtomicLong dropCounter = new AtomicLong(0L);

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "head", long.class);
            TAIL_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public LockFreeRingBuffer(int requestedCapacity) {
        if (requestedCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than zero.");
        }
        // Round up to nearest power of two
        int cap = 1;
        while (cap < requestedCapacity) {
            cap <<= 1;
        }
        this.capacity = cap;
        this.mask = cap - 1;
        this.buffer = new Object[this.capacity];
        this.sequence = new long[this.capacity];

        // Initialize sequence array values to correspond to slot indices
        for (int i = 0; i < this.capacity; i++) {
            this.sequence[i] = i;
        }

        this.head = 0L;
        this.tail = 0L;
    }

    /**
     * Returns the capacity of the ring buffer.
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Returns the total number of dropped elements due to ring overflows.
     */
    public long getDropCount() {
        return dropCounter.get();
    }

    /**
     * Inserts the specified element into this ring buffer if it is possible to do so
     * immediately without violating capacity invariants.
     *
     * @param e the element to add
     * @return true if the element was added, false if the buffer was full
     */
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException("Element cannot be null.");
        }

        long currentTail = (long) TAIL_HANDLE.getAcquire(this);
        while (true) {
            int idx = (int) (currentTail & mask);
            long seq = (long) UNSAFE_GET_LONG(sequence, idx);
            long diff = seq - currentTail;

            if (diff == 0) {
                // Slot is free, attempt to claim it by advancing tail
                if (TAIL_HANDLE.compareAndSet(this, currentTail, currentTail + 1)) {
                    buffer[idx] = e;
                    // Publish sequence enabling consumer visibility
                    UNSAFE_PUT_LONG(sequence, idx, currentTail + 1);
                    return true;
                }
            } else if (diff < 0) {
                // Buffer is full (Invariant 1: Tail - Head <= Capacity)
                dropCounter.incrementAndGet();
                return false;
            } else {
                // Tail moved concurrently, re-read tail
                currentTail = (long) TAIL_HANDLE.getAcquire(this);
            }
        }
    }

    /**
     * Retrieves and removes the head of this ring buffer, or returns null if empty.
     *
     * @return the head element, or null if the ring buffer is empty
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        while (true) {
            int idx = (int) (currentHead & mask);
            long seq = (long) UNSAFE_GET_LONG(sequence, idx);
            long diff = seq - (currentHead + 1);

            if (diff == 0) {
                // Slot contains data, attempt to claim it by advancing head
                if (HEAD_HANDLE.compareAndSet(this, currentHead, currentHead + 1)) {
                    E e = (E) buffer[idx];
                    buffer[idx] = null; // Clear reference for GC / zeroization equivalent
                    // Publish sequence enabling producer reuse
                    UNSAFE_PUT_LONG(sequence, idx, currentHead + mask + 1);
                    return e;
                }
            } else if (diff < 0) {
                // Buffer is empty
                return null;
            } else {
                // Head moved concurrently, re-read head
                currentHead = (long) HEAD_HANDLE.getAcquire(this);
            }
        }
    }

    /**
     * Returns the approximate number of elements currently stored in the ring buffer.
     */
    public int size() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        long currentTail = (long) TAIL_HANDLE.getAcquire(this);
        long size = currentTail - currentHead;
        if (size < 0) {
            return 0;
        }
        return (int) Math.min(size, capacity);
    }

    /**
     * Checks whether the ring buffer is empty.
     */
    public boolean isEmpty() {
        return ((long) HEAD_HANDLE.getAcquire(this)) == ((long) TAIL_HANDLE.getAcquire(this));
    }

    // Helper wrappers to guarantee volatile array access semantics without Unsafe API direct dependencies 
    // where VarHandles suffice or explicit array volatile operations.
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

    private static long UNSAFE_GET_LONG(long[] arr, int idx) {
        return (long) LONG_ARRAY_HANDLE.getAcquire(arr, idx);
    }

    private static void UNSAFE_PUT_LONG(long[] arr, int idx, long val) {
        LONG_ARRAY_HANDLE.setRelease(arr, idx, val);
    }
}