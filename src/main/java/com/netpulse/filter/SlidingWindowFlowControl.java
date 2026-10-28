package com.netpulse.filter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class SlidingWindowFlowControl {

    private static final VarHandle HEAD_TIMESTAMP_HANDLE;
    private static final VarHandle TAIL_TIMESTAMP_HANDLE;
    private static final VarHandle WINDOW_SLOTS_HANDLE;
    private static final VarHandle DROP_COUNT_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_TIMESTAMP_HANDLE = l.findVarHandle(SlidingWindowFlowControl.class, "headTimestamp", long.class);
            TAIL_TIMESTAMP_HANDLE = l.findVarHandle(SlidingWindowFlowControl.class, "tailTimestamp", long.class);
            WINDOW_SLOTS_HANDLE = l.findVarHandle(SlidingWindowFlowControl.class, "windowSlots", long[].class);
            DROP_COUNT_HANDLE = l.findVarHandle(SlidingWindowFlowControl.class, "dropCount", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long windowDurationMs;
    private final int numSlots;
    private final long slotDurationMs;
    private final long maxPacketsPerWindow;

    // Cache line padding via volatile fields and structural separation
    @SuppressWarnings("unused")
    private volatile long p1, p2, p3, p4, p5, p6, p7;

    private volatile long headTimestamp;
    private volatile long tailTimestamp;
    private final long[] windowSlots;

    @SuppressWarnings("unused")
    private volatile long p8, p9, p10, p11, p12, p13, p14;

    private volatile long dropCount;

    @SuppressWarnings("unused")
    private volatile long p15, p16, p17, p18, p19, p20, p21;

    public SlidingWindowFlowControl(long windowDurationMs, int numSlots, long maxPacketsPerWindow) {
        if (windowDurationMs <= 0) {
            throw new IllegalArgumentException("windowDurationMs must be positive");
        }
        if (numSlots <= 0 || (numSlots & (numSlots - 1)) != 0) {
            throw new IllegalArgumentException("numSlots must be a positive power of two");
        }
        if (maxPacketsPerWindow <= 0) {
            throw new IllegalArgumentException("maxPacketsPerWindow must be positive");
        }

        this.windowDurationMs = windowDurationMs;
        this.numSlots = numSlots;
        this.slotDurationMs = windowDurationMs / numSlots;
        this.maxPacketsPerWindow = maxPacketsPerWindow;
        this.windowSlots = new long[numSlots];

        long now = System.currentTimeMillis();
        this.headTimestamp = now;
        this.tailTimestamp = now;
        this.dropCount = 0L;
    }

    public boolean allowPacket(long currentTimeMs) {
        advanceWindow(currentTimeMs);

        long currentTotal = sumWindowSlots();
        if (currentTotal >= maxPacketsPerWindow) {
            long drops = (long) DROP_COUNT_HANDLE.getAndAdd(this, 1L);
            return false;
        }

        incrementCurrentSlot(currentTimeMs);
        return true;
    }

    public long getDropCount() {
        return (long) DROP_COUNT_HANDLE.getAcquire(this);
    }

    public void reset() {
        long now = System.currentTimeMillis();
        HEAD_TIMESTAMP_HANDLE.setRelease(this, now);
        TAIL_TIMESTAMP_HANDLE.setRelease(this, now);
        
        long[] slots = (long[]) WINDOW_SLOTS_HANDLE.getAcquire(this);
        for (int i = 0; i < slots.length; i++) {
            slots[i] = 0L;
        }
        DROP_COUNT_HANDLE.setRelease(this, 0L);
    }

    private void advanceWindow(long currentTimeMs) {
        long tail = (long) TAIL_TIMESTAMP_HANDLE.getAcquire(this);
        long elapsed = currentTimeMs - tail;

        if (elapsed <= 0) {
            return;
        }

        if (elapsed >= windowDurationMs) {
            // Entire window has elapsed, zero out all slots and reset anchor
            long[] slots = (long[]) WINDOW_SLOTS_HANDLE.getAcquire(this);
            for (int i = 0; i < slots.length; i++) {
                slots[i] = 0L;
            }
            TAIL_TIMESTAMP_HANDLE.setRelease(this, currentTimeMs);
            HEAD_TIMESTAMP_HANDLE.setRelease(this, currentTimeMs);
            return;
        }

        // Advance slot by slot based on elapsed time increments
        long slotsToAdvance = elapsed / slotDurationMs;
        if (slotsToAdvance <= 0) {
            return;
        }

        long[] slots = (long[]) WINDOW_SLOTS_HANDLE.getAcquire(this);
        int mask = numSlots - 1;

        for (long i = 0; i < slotsToAdvance; i++) {
            long currentTail = (long) TAIL_TIMESTAMP_HANDLE.getAcquire(this);
            int slotIndex = (int) ((currentTail / slotDurationMs) & mask);
            slots[slotIndex] = 0L;
            TAIL_TIMESTAMP_HANDLE.setRelease(this, currentTail + slotDurationMs);
        }

        HEAD_TIMESTAMP_HANDLE.setRelease(this, currentTimeMs);
    }

    private void incrementCurrentSlot(long currentTimeMs) {
        long[] slots = (long[]) WINDOW_SLOTS_HANDLE.getAcquire(this);
        int mask = numSlots - 1;
        int slotIndex = (int) ((currentTimeMs / slotDurationMs) & mask);
        
        // Safe volatile-equivalent update on the local array element via synchronized or direct slot tracking
        synchronized (slots) {
            slots[slotIndex]++;
        }
    }

    private long sumWindowSlots() {
        long[] slots = (long[]) WINDOW_SLOTS_HANDLE.getAcquire(this);
        long sum = 0L;
        synchronized (slots) {
            for (int i = 0; i < slots.length; i++) {
                sum += slots[i];
            }
        }
        return sum;
    }
}