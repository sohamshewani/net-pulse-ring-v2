import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public final class NetPulseRingMicrobenchmark {

    private static final int WARMUP_ITERATIONS = 500_000;
    private static final int MEASUREMENT_ITERATIONS = 5_000_000;
    private static final int BATCH_SIZE = 10_005;

    // Simulated 'net-pulse-ring' transport ring buffer and state
    private static final class NetPulseRing {
        private final long[] ringBuffer;
        private final int mask;
        private int head;
        private int tail;

        NetPulseRing(int capacityBits) {
            int capacity = 1 << capacityBits;
            this.ringBuffer = new long[capacity];
            this.mask = capacity - 1;
            this.head = 0;
            this.tail = 0;
        }

        public boolean offer(long pulseSignal) {
            int currentTail = tail;
            int nextTail = (currentTail + 1) & mask;
            if (nextTail == head) {
                return false; // Ring full
            }
            ringBuffer[currentTail] = pulseSignal;
            tail = nextTail;
            return true;
        }

        public long poll() {
            int currentHead = head;
            if (currentHead == tail) {
                return -1L; // Ring empty
            }
            long pulseSignal = ringBuffer[currentHead];
            head = (currentHead + 1) & mask;
            return pulseSignal;
        }
    }

    public static void main(String[] args) {
        // Initialize ring with 2^16 capacity (65536)
        NetPulseRing ring = new NetPulseRing(16);

        long[] latencies = new long[MEASUREMENT_ITERATIONS];

        // -----------------------------------------------------------------
        // 1. WARMUP PHASE
        // -----------------------------------------------------------------
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            long signal = ThreadLocalRandom.current().nextLong();
            while (!ring.offer(signal)) {
                // spin if full
            }
            long polled = ring.poll();
            if (polled != signal) {
                throw new AssertionError("Warmup data corruption");
            }
        }

        // -----------------------------------------------------------------
        // 2. MEASUREMENT PHASE
        // -----------------------------------------------------------------
        long startTime = System.nanoTime();

        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long signal = i + 1;
            
            long startNano = System.nanoTime();
            
            // Perform ring round-trip (push + pull representing a pulse operation)
            while (!ring.offer(signal)) {
                Thread.onSpinWait();
            }
            long result = ring.poll();
            
            long endNano = System.nanoTime();

            if (result != signal) {
                throw new AssertionError("Measurement data corruption");
            }

            latencies[i] = endNano - startNano;
        }

        long endTime = System.nanoTime();
        long totalDurationNanos = endTime - startTime;

        // -----------------------------------------------------------------
        // 3. METRICS PROCESSING
        // -----------------------------------------------------------------
        Arrays.sort(latencies);

        double totalSeconds = totalDurationNanos / 1_000_000_000.0;
        double opsPerSec = MEASUREMENT_ITERATIONS / totalSeconds;

        long p50 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.50)];
        long p90 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.90)];
        long p99 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.99)];
        long p999 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.999)];
        long max = latencies[MEASUREMENT_ITERATIONS - 1];

        // -----------------------------------------------------------------
        // 4. FORMATTED TABULAR OUTPUT
        // -----------------------------------------------------------------
        System.out.println("==================================================================");
        System.out.println("            NET-PULSE-RING MICROBENCHMARK RESULTS                 ");
        System.out.println("==================================================================");
        System.out.printf(" %-24s | %-15s | %-15s %n", "Metric", "Value", "Unit");
        System.out.println("------------------------------------------------------------------");
        System.out.printf(" %-24s | %-15d | %-15s %n", "Warmup Iterations", WARMUP_ITERATIONS, "ops");
        System.out.printf(" %-24s | %-15d | %-15s %n", "Measurement Iterations", MEASUREMENT_ITERATIONS, "ops");
        System.out.printf(" %-24s | %-15.2f | %-15s %n", "Total Duration", totalSeconds, "seconds");
        System.out.printf(" %-24s | %-15.2f | %-15s %n", "Throughput (Ops/Sec)", opsPerSec, "ops/sec");
        System.out.println("------------------------------------------------------------------");
        System.out.printf(" %-24s | %-15.3f | %-15s %n", "Latency p50", p50 / 1000.0, "microseconds");
        System.out.printf(" %-24s | %-15.3f | %-15s %n", "Latency p90", p90 / 1000.0, "microseconds");
        System.out.printf(" %-24s | %-15.3f | %-15s %n", "Latency p99", p99 / 1000.0, "microseconds");
        System.out.printf(" %-24s | %-15.3f | %-15s %n", "Latency p99.9", p999 / 1000.0, "microseconds");
        System.out.printf(" %-24s | %-15.3f | %-15s %n", "Latency Max", max / 1000.0, "microseconds");
        System.out.println("==================================================================");
    }
}