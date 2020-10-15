package com.ddcoding.core.stream.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ShutdownableThread.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private Thread thread = null;
    private Boolean isInterruptible = true;

    public ShutdownableThread(Boolean isInterruptible) {
        this.isInterruptible = isInterruptible;
    }

    public void awaitShutdown() throws InterruptedException {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("shutting down");
            isRunning.set(false);
            if (isInterruptible) {
                thread.interrupt();
            }
        }
        shutdownLatch.await();
        logger.info("Shutdown completed");
    }

    protected abstract void doWork() throws InterruptedException;

    @Override
    public void run() {
        thread = Thread.currentThread();
        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (Throwable t) {
            if (isRunning.get()) {
                logger.error("Error due to ", t);
            }
        }

        shutdownLatch.countDown();
        logger.info("stopped.");
    }
}
