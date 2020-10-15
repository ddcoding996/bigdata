package com.ddcoding.core.stream.manager;

import com.ddcoding.core.stream.sink.Sink;
import com.ddcoding.core.stream.task.SinkTask;
import com.ddcoding.core.stream.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SinkManager {

    private final Logger logger = LoggerFactory.getLogger(SinkManager.class);
    private int sinkerCount = 1;
    private double factor = 2;
    private final ExecutorService threadPool;
    private final ArrayBlockingQueue<Task> pendingQueue;
    private final List<Sink> sinkers;
    private final List<SinkThread> sinkThreads;

    public SinkManager(List<Sink> sinkers, int sinkerCount, double factor) {
        this.sinkerCount = sinkerCount;
        this.factor = factor;
        this.threadPool = Executors.newFixedThreadPool(sinkerCount);
        this.pendingQueue = new ArrayBlockingQueue<Task>((int) ((Math.max(1, (sinkerCount * factor)))));
        this.sinkThreads = new ArrayList<SinkThread>(sinkerCount);
        this.sinkers = sinkers;
        for (int i = 0; i < sinkerCount; i++) {
            this.sinkThreads.add(new SinkThread(this.pendingQueue, this.sinkers, true));
        }
    }

    public Boolean submitTask(Task task, long timeoutMs) throws InterruptedException {
        logger.debug("Submit task to SinkManager {}", task);
        return pendingQueue.offer(task, timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void start() {
        logger.info("Sink Manager starts, submit {} sink thread, factor: {}", sinkerCount, factor);
        for (SinkThread thread : sinkThreads) {
            threadPool.submit(thread);
        }
    }

    public void shutdown() throws InterruptedException {
        for (SinkThread thread : sinkThreads) {
            thread.awaitShutdown();
            threadPool.shutdownNow();
            threadPool.awaitTermination(2, TimeUnit.SECONDS);
            logger.info("SinkManager closed");
        }
    }

    private class SinkThread extends ShutdownableThread {

        private final static int POLL_TIMEOUT_MS = 300;
        private BlockingQueue<Task> queue;
        private List<Sink> sinkers;

        public SinkThread(BlockingQueue<Task> queue, List<Sink> sinkers, Boolean isInterruptible) {
            super(isInterruptible);
            this.queue = queue;
            this.sinkers = sinkers;
        }

        @Override
        protected void doWork() {
            Task task = null;
            try {
                task = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error("Interupt error: ", e);
            }
            if (task != null) {
                logger.debug("fetch task from pendingQueue {}", task);
                if (task instanceof SinkTask) {
                    SinkTask sinkTask = (SinkTask) task;
                    if (!sinkTask.getTaskinfo().isAlive()) {
                        logger.warn("partition has been unregistered or outdated for {}", sinkTask.getTaskinfo());
                    } else {
                        for (Sink sink : sinkers) {
                            try {
                                sink.process(sinkTask);
                            } catch (Exception e) {
                                logger.error("sink process failed. err: ", e);
                            }
                        }
                    }
                }
                task.run();
            }
        }
    }
}