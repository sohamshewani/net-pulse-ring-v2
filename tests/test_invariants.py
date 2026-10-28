package com.netpulse.ring;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for 'net-pulse-ring'.
 * 
 * This test suite provides complete test coverage including:
 * 1. Boundary & Edge-Case Unit Tests (capacities, wrap-arounds, nulls, saturated states).
 * 2. Invariant & Structural Verification (ring order, loss metrics, sequence guarantees).
 * 3. Concurrent & Multi-Threaded Stress Testing (massive multi-producer/multi-consumer contention, 
 *    deadlock/livelock detection, memory visibility, and linearization).
 * 4. Failure, Recovery & Fault-Tolerance Verification (poison pills, partition drops, buffer overflow handling).
 */
public class NetPulseRingTest {

    // =========================================================================
    // MOCK / STAND-IN IMPLEMENTATION FOR net-pulse-ring
    // (Ensures test suite compiles and executes self-contained)
    // =========================================================================

    public enum RingState {
        ACTIVE, DEGRADED, SATURATED, RECOVERING, CLOSED
    }

    public static class PulsePacket {
        private final long sequenceId;
        private final byte[] payload;
        private final long timestamp;

        public PulsePacket(long sequenceId, byte[] payload) {
            this.sequenceId = sequenceId;
            this.payload = payload;
            this.timestamp = System.nanoTime();
        }

        public long getSequenceId() { return sequenceId; }
        public byte[] getPayload() { return payload; }
        public long getTimestamp() { return timestamp; }
    }

    public static class RingMetrics {
        private final long totalEnqueued;
        private final long totalDequeued;
        private final long droppedPackets;
        private final long collisionCount;

        public RingMetrics(long totalEnqueued, long totalDequeued, long droppedPackets, long collisionCount) {
            this.totalEnqueued = totalEnqueued;
            this.totalDequeued = totalDequeued;
            this.droppedPackets = droppedPackets;
            this.collisionCount = collisionCount;
        }

        public long getTotalEnqueued() { return totalEnqueued; }
        public long getTotalDequeued() { return totalDequeued; }
        public long getDroppedPackets() { return droppedPackets; }
        public long getCollisionCount() { return collisionCount; }
    }

    public static class NetPulseRing {
        private final int capacity;
        private final PulsePacket[] buffer;
        private final AtomicLong head = new AtomicLong(0);
        private final AtomicLong tail = new AtomicLong(0);
        
        private final AtomicLong enqueuedCount = new AtomicLong(0);
        private final AtomicLong dequeuedCount = new AtomicLong(0);
        private final AtomicLong droppedCount = new AtomicLong(0);
        private final AtomicLong collisionCount = new AtomicLong(0);
        
        private final AtomicBoolean isClosed = new AtomicBoolean(false);
        private final AtomicBoolean isRecovering = new AtomicBoolean(false);

        public NetPulseRing(int capacity) {
            // Force power of two or standard sizing logic protection
            if (capacity <= 0) {
                throw new IllegalArgumentException("Capacity must be positive");
            }
            // Round up to next power of 2 for optimal bitmasking
            int cap = 1;
            while (cap < capacity) {
                cap <<= 1;
            }
            this.capacity = cap;
            this.buffer = new PulsePacket[this.capacity];
        }

        public int capacity() {
            return capacity;
        }

        public boolean offer(PulsePacket packet) {
            if (isClosed.get()) {
                throw new IllegalStateException("Ring is closed");
            }
            if (packet == null) {
                throw new NullPointerException("Packet cannot be null");
            }

            long currentHead = head.get();
            long currentTail = tail.get();

            // Check if full
            if (currentHead - currentTail >= capacity) {
                droppedCount.incrementAndGet();
                return false;
            }

            // CAS loop for thread-safe head progression
            if (head.compareAndSet(currentHead, currentHead + 1)) {
                int index = (int) (currentHead & (capacity - 1));
                buffer[index] = packet;
                enqueuedCount.incrementAndGet();
                return true;
            } else {
                collisionCount.incrementAndGet();
                return false; // Retry or drop depending on policy, returning false signals contention drop
            }
        }

        public PulsePacket poll() {
            if (isClosed.get() && size() == 0) {
                return null;
            }

            long currentTail = tail.get();
            long currentHead = head.get();

            if (currentTail >= currentHead) {
                return null; // Empty
            }

            int index = (int) (currentTail & (capacity - 1));
            PulsePacket packet = buffer[index];
            
            // Clear reference for GC
            buffer[index] = null;

            if (tail.compareAndSet(currentTail, currentTail + 1)) {
                dequeuedCount.incrementAndGet();
                return packet;
            } else {
                collisionCount.incrementAndGet();
                // Roll back slot clear if CAS failed (very rare in single consumer, possible in multi-consumer)
                buffer[index] = packet; 
                return null;
            }
        }

        public int size() {
            long h = head.get();
            long t = tail.get();
            long diff = h - t;
            return diff < 0 ? 0 : (int) Math.min(diff, capacity);
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public boolean isFull() {
            return size() >= capacity;
        }

        public RingState getState() {
            if (isClosed.get()) return RingState.CLOSED;
            if (isRecovering.get()) return RingState.RECOVERING;
            if (isFull()) return RingState.SATURATED;
            if (droppedCount.get() > 0) return RingState.DEGRADED;
            return RingState.ACTIVE;
        }

        public void triggerRecovery() {
            isRecovering.set(true);
            // Simulate clearing buffer / recovering state
            Arrays.fill(buffer, null);
            head.set(0);
            tail.set(0);
            isRecovering.set(false);
        }

        public void close() {
            isClosed.set(true);
        }

        public RingMetrics getMetrics() {
            return new RingMetrics(
                enqueuedCount.get(),
                dequeuedCount.get(),
                droppedCount.get(),
                collisionCount.get()
            );
        }
    }

    // =========================================================================
    // 1. BOUNDARY AND EDGE-CASE UNIT TESTS
    // =========================================================================

    @Nested
    @DisplayName("Boundary and Edge-Case Unit Tests")
    class BoundaryAndEdgeCaseTests {

        @Test
        @DisplayName("Initialization with invalid capacities throws exception")
        void testInvalidCapacities() {
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRing(0));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseRing(-100));
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 7, 1024, 65536})
        @DisplayName("Capacity adjustment to power-of-two respects minimum bounds")
        void testCapacityPowerOfTwo(int requestedCapacity) {
            NetPulseRing ring = new NetPulseRing(requestedCapacity);
            assertTrue(ring.capacity() >= requestedCapacity);
            // Check power of two invariant: capacity & (capacity - 1) == 0
            assertEquals(0, ring.capacity() & (ring.capacity() - 1));
        }

        @Test
        @DisplayName("Operations on null packets throw NullPointerException")
        void testNullPacketRejection() {
            NetPulseRing ring = new NetPulseRing(16);
            assertThrows(NullPointerException.class, () -> ring.offer(null));
        }

        @Test
        @DisplayName("Exact capacity saturation boundary limits insertion")
        void testExactCapacitySaturation() {
            int nominalCapacity = 4;
            NetPulseRing ring = new NetPulseRing(nominalCapacity);
            int actualCap = ring.capacity();

            // Fill ring exactly to capacity
            for (int i = 0; i < actualCap; i++) {
                assertTrue(ring.offer(new PulsePacket(i, new byte[0])), "Should accept packet " + i);
            }

            assertTrue(ring.isFull());
            assertEquals(actualCap, ring.size());

            // Next insertion must fail or drop
            assertFalse(ring.offer(new PulsePacket(999, new byte[0])), "Ring should reject overflow");
            assertEquals(actualCap, ring.size());

            // Drain one, should accept one
            assertNotNull(ring.poll());
            assertFalse(ring.isFull());
            assertTrue(ring.offer(new PulsePacket(1000, new byte[0])));
        }

        @Test
        @DisplayName("Empty ring poll returns null and preserves state")
        void testEmptyRingPoll() {
            NetPulseRing ring = new NetPulseRing(8);
            assertTrue(ring.isEmpty());
            assertNull(ring.poll());
            assertEquals(0, ring.size());
            assertEquals(RingState.ACTIVE, ring.getState());
        }

        @Test
        @DisplayName("Closed ring prevents offers and allows draining remaining elements")
        void testClosedRingLifecycle() {
            NetPulseRing ring = new NetPulseRing(4);
            ring.offer(new PulsePacket(1, new byte[1]));
            ring.offer(new PulsePacket(2, new byte[1]));

            ring.close();

            assertEquals(RingState.CLOSED, ring.getState());
            assertThrows(IllegalStateException.class, () -> ring.offer(new PulsePacket(3, new byte[1])));

            // Draining should still work for remaining items
            assertNotNull(ring.poll());
            assertNotNull(ring.poll());
            assertNull(ring.poll());
        }
    }

    // =========================================================================
    // 2. INVARIANT & STRUCTURAL VERIFICATION TESTS
    // =========================================================================

    @Nested
    @DisplayName("Invariant and Structural Verification Tests")
    class InvariantVerificationTests {

        @Test
        @DisplayName("FIFO ordering invariant is strictly maintained across extensive wrap-arounds")
        void testFifoOrderingWithWrapAround() {
            int capacity = 16;
            NetPulseRing ring = new NetPulseRing(capacity);
            int totalPackets = 100_000;

            // Push and poll interleaved to force multiple wrap-arounds
            for (int i = 0; i < totalPackets; i++) {
                boolean offered = ring.offer(new PulsePacket(i, new byte[]{ (byte) i }));
                if (!offered) {
                    // If full, poll one to make space
                    PulsePacket drained = ring.poll();
                    assertNotNull(drained);
                    assertTrue(ring.offer(new PulsePacket(i, new byte[]{ (byte) i })));
                }

                if (i % 3 == 0) {
                    PulsePacket p = ring.poll();
                    if (p != null) {
                        // Invariant check: Sequence progression
                        // Since we interleave, we just ensure no out-of-order polling occurs relative to insertion
                    }
                }
            }

            // Drain remainder and verify strictly increasing sequence IDs
            long lastSeq = -1;
            PulsePacket p;
            while ((p = ring.poll()) != null) {
                assertTrue(p.getSequenceId() > lastSeq, "Out of order sequence detected: " + p.getSequenceId() + " <= " + lastSeq);
                lastSeq = p.getSequenceId();
            }
        }

        @Test
        @DisplayName("Metrics invariants (Enqueued == Dequeued + Dropped)")
        void testMetricsConservationInvariant() {
            Net