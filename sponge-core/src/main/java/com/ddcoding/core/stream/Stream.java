package com.ddcoding.core.stream;

import com.ddcoding.core.stream.manager.SinkManager;
import com.ddcoding.core.stream.manager.WorkerManager;
import com.ddcoding.core.stream.sink.ElasticSearchSink;
import com.ddcoding.core.stream.sink.Sink;
import com.ddcoding.core.stream.source.KafkaSourceContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Stream {

    private static final Logger logger = LoggerFactory.getLogger(Stream.class);

    private final WorkerManager workManager;
    private final SinkManager sinkManager;
    private final KafkaSourceContainer kafkaSourceContainer;
    private final CountDownLatch shutdownLatch;

    public Stream(StreamConfig config) {
        List<Sink> sinks = new ArrayList<Sink>();
        sinks.add(new ElasticSearchSink(config));
        this.workManager = new WorkerManager(config.getWorkCount(), config.getWorkFactor());
        this.sinkManager = new SinkManager(sinks, config.getSinkCount(), config.getSinkFactor());
        this.kafkaSourceContainer = new KafkaSourceContainer(workManager,
                config.getKafkaSourceParallelNum(),
                config.getKafkaConsumerConf(),
                config.getZkAddr());
        this.shutdownLatch = new CountDownLatch(1);
    }

    public void start() {
        logger.info("Stream starting process.");
        workManager.start();
        sinkManager.start();
        kafkaSourceContainer.start();
        logger.info("started.");
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
        kafkaSourceContainer.commitAndClose();
    }

    public void shutdown() throws InterruptedException {
        if (shutdownLatch.getCount() > 0) {
            kafkaSourceContainer.shutdown();
            sinkManager.shutdown();
            workManager.shutdown();
            shutdownLatch.countDown();
            logger.info("Driver is closed.");
        }
    }
}
