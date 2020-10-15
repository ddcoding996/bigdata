package com.ddcoding.core.stream.source;


import com.ddcoding.core.stream.manager.ShutdownableThread;
import com.ddcoding.core.stream.manager.WorkerManager;
import com.ddcoding.core.stream.task.WorkTask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaSourceContainer extends SourceContainer {

    private final KafkaConsumerConf.ConsumerConf kafkaConsumerConf;

    private String zkAddr;
    private final List<SourceThread> kafkaSources;
    private final ExecutorService threadPool;

    public KafkaSourceContainer(WorkerManager workerManager, int parallelCount, KafkaConsumerConf.ConsumerConf kafkaConsumerConf,
                                String zkAddr) {
        super(workerManager);
        this.kafkaConsumerConf = kafkaConsumerConf;
        this.setZkAddr(zkAddr);
        this.kafkaSources = new ArrayList<SourceThread>(parallelCount);
        this.threadPool = Executors.newFixedThreadPool(parallelCount);
    }

    private class SourceThread extends ShutdownableThread {

        public SourceThread(Boolean isInterruptible) {
            super(isInterruptible);
        }

        private final KafkaSource source = new KafkaSource_1010(kafkaConsumerConf);
        private static final int SUBMIT_TIMEOUT_MS = 5000;

        private Iterator<WorkTask> lastIterable = new ArrayList<WorkTask>().iterator();
        private Boolean paused = false;

        @Override
        protected void doWork() throws InterruptedException {
            List<WorkTask> tasks = source.fetchAndCreateTasks();
            Iterator<WorkTask> iterable;
            if (tasks.isEmpty()) {
                iterable = lastIterable;
            } else {
                paused = false;
                iterable = tasks.iterator();
            }

            while (iterable.hasNext()) {
                WorkTask workTask = iterable.next();
                Boolean result = getWorkerManager().submitTask(workTask, SUBMIT_TIMEOUT_MS);
                if (result) {
                    if (paused) {
                        source.resume();
                        paused = false;
                    }
                } else {
                    if (!paused) {
                        source.pause();
                        paused = true;
                    }
                    iterable.remove();
                }
            }
            lastIterable = iterable;

            source.commitOffset();
        }

        private void start() {
            source.start();
        }

        private void shutdown() throws InterruptedException {
            source.wakeUp();
            awaitShutdown();
        }

        private void closeSource() {
            source.close();
        }
    }

    @Override
    public void start() {
        for (SourceThread kafkaSource : kafkaSources) {
            kafkaSource.start();
            threadPool.submit(kafkaSource);
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        for (SourceThread kafkaSource : kafkaSources) {
            kafkaSource.shutdown();
        }
        threadPool.awaitTermination(2, TimeUnit.SECONDS);
    }

    public void commitAndClose() {
        for (SourceThread kafkaSource : kafkaSources) {
            kafkaSource.closeSource();
        }
    }

    public String getZkAddr() {
        return zkAddr;
    }

    public void setZkAddr(String zkAddr) {
        this.zkAddr = zkAddr;
    }
}
