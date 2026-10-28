package com.netpulse.engine;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class NonBlockingPollLoop implements Runnable {

    private final Selector selector;
    private final LockFreeRingBuffer rxRingBuffer;
    private final SlidingWindowFlowControl flowControl;
    private final MemoryArena memoryArena;
    private final AtomicBoolean running;
    private final int spinLimit;
    private volatile Thread pollerThread;

    public NonBlockingPollLoop(
            final LockFreeRingBuffer rxRingBuffer,
            final SlidingWindowFlowControl flowControl,
            final MemoryArena memoryArena,
            final int spinLimit) throws IOException {
        this.rxRingBuffer = rxRingBuffer;
        this.flowControl = flowControl;
        this.memoryArena = memoryArena;
        this.spinLimit = spinLimit;
        this.selector = Selector.open();
        this.running = new AtomicBoolean(false);
    }

    public Selector getSelector() {
        return this.selector;
    }

    @Override
    public void run() {
        this.pollerThread = Thread.currentThread();
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        int spins = 0;
        final Selector sel = this.selector;

        try {
            while (this.running.get()) {
                int selectedKeys = 0;

                if (spins < this.spinLimit) {
                    selectedKeys = sel.selectNow();
                    if (selectedKeys == 0) {
                        spins++;
                        Thread.onSpinWait();
                        continue;
                    }
                } else {
                    selectedKeys = sel.select(1);
                }

                if (selectedKeys > 0) {
                    spins = 0;
                    final Set<SelectionKey> keys = sel.selectedKeys();
                    final Iterator<SelectionKey> iterator = keys.iterator();

                    while (iterator.hasNext()) {
                        final SelectionKey key = iterator.next();
                        iterator.remove();

                        if (!key.isValid()) {
                            continue;
                        }

                        try {
                            if (key.isReadable()) {
                                processRead(key);
                            }
                        } catch (final IOException e) {
                            handleChannelException(key, e);
                        }
                    }
                } else {
                    spins = 0;
                }
            }
        } catch (final Throwable t) {
            handleFatalLoopError(t);
        } finally {
            cleanup();
        }
    }

    private void processRead(final SelectionKey key) throws IOException {
        final SocketChannel channel = (SocketChannel) key.channel();
        
        if (!this.flowControl.allowIngress(System.nanoTime())) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            this.flowControl.recordDrop();
            return;
        }

        final long address = this.memoryArena.allocate();
        if (address == 0L) {
            this.flowControl.triggerBackpressure();
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            return;
        }

        final PacketFrame frame = this.memoryArena.wrapDirect(address);
        final int bytesRead = frame.readFromChannel(channel);

        if (bytesRead == -1) {
            this.memoryArena.release(address);
            key.cancel();
            channel.close();
            return;
        }

        if (bytesRead > 0) {
            this.flowControl.recordIngress(bytesRead, System.nanoTime());
            if (!this.rxRingBuffer.offer(frame)) {
                this.flowControl.recordDrop();
                this.memoryArena.release(address);
            }
        } else {
            this.memoryArena.release(address);
        }
    }

    private void handleChannelException(final SelectionKey key, final IOException e) {
        try {
            key.cancel();
            if (key.channel() != null) {
                key.channel().close();
            }
        } catch (final IOException ignored) {
            // Suppress secondary close exceptions
        }
        this.flowControl.recordDrop();
    }

    private void handleFatalLoopError(final Throwable t) {
        // Telemetry logging hook for unhandled system-level poll failures
    }

    public void shutdown() {
        if (this.running.compareAndSet(true, false)) {
            try {
                this.selector.wakeup();
            } catch (final Throwable ignored) {
                // Ignore wakeup failures during shutdown
            }
            final Thread t = this.pollerThread;
            if (t != null && t != Thread.currentThread()) {
                LockSupport.unpark(t);
            }
        }
    }

    private void cleanup() {
        try {
            final Set<SelectionKey> keys = this.selector.keys();
            for (final SelectionKey key : keys) {
                try {
                    key.cancel();
                    if (key.channel() != null) {
                        key.channel().close();
                    }
                } catch (final IOException ignored) {
                }
            }
            this.selector.close();
        } catch (final IOException ignored) {
        }
    }
}