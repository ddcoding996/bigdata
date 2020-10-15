package com.ddcoding.core.stream.manager;

import com.ddcoding.core.stream.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class WorkerManager {

    private final Logger logger = LoggerFactory.getLogger(WorkerManager.class);
    private int workerCount = 1;
    private double factor = 0.5;
    private final ExecutorService threadPool;
    private final ArrayBlockingQueue<Task> pendingQueue;
    private final List<WorkerThread> workerThreads;

    public WorkerManager(int workerCount, double factor) {
        this.workerCount = workerCount;
        this.factor = factor;
        this.threadPool = Executors.newFixedThreadPool(workerCount);
        this.pendingQueue = new ArrayBlockingQueue<Task>((int) ((Math.max(1, (workerCount * factor)))));
        this.workerThreads = new ArrayList<WorkerThread>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            this.workerThreads.add(new WorkerThread(this.pendingQueue, true));
        }
    }

    /**
     * 向队列添加task
     *
     * @param task
     * @param timeoutMs
     * @return
     * @throws InterruptedException
     */
    public Boolean submitTask(Task task, long timeoutMs) throws InterruptedException {
        logger.debug("Submit task to taskManager {}", task);
        return pendingQueue.offer(task, timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void start() {
        logger.info("Task Manager starts, submit {} work thread, factor: {}", workerCount, factor);
        for (WorkerThread thread : workerThreads) {
            threadPool.submit(thread);
        }
    }

    public void shutdown() throws InterruptedException {
        for (WorkerThread thread : workerThreads) {
            thread.awaitShutdown();
            threadPool.shutdownNow();
            threadPool.awaitTermination(2, TimeUnit.SECONDS);
            logger.info("TaskManager closed");
        }
    }

    private class WorkerThread extends ShutdownableThread {

        private final static int POLL_TIMEOUT_MS = 300;
        private BlockingQueue<Task> queue;

        public WorkerThread(BlockingQueue<Task> queue, Boolean isInterruptible) {
            super(isInterruptible);
            this.queue = queue;
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
                task.run();
            }
        }
    }
}
