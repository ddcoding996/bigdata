package com.ddcoding.core.stream.source;

import com.ddcoding.core.stream.partition.PartitionInfo;
import com.ddcoding.core.stream.task.TaskInfo;
import com.ddcoding.core.stream.task.WorkTask;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public abstract class KafkaSource {

    private final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

    protected KafkaConsumerConf.ConsumerConf kafkaConsumerConf;

    public KafkaSource(KafkaConsumerConf.ConsumerConf kafkaConsumerConf) {
        this.kafkaConsumerConf = kafkaConsumerConf;
    }

    protected KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            kafkaConsumerConf.getProperties());
    protected KafkaOffsetCommitter committer = new KafkaOffsetCommitter();
    protected String consumerID = consumer.metrics().entrySet().iterator().next().getKey().tags().get("client-id");
    protected Map<Integer, Long> pidToTaskId = new HashMap<Integer, Long>();
    protected Map<Integer, PartitionInfo> partitionInfos = new HashMap<Integer, PartitionInfo>();

    public KafkaOffsetCommitter getCommitter() {
        return committer;
    }

    public List<Integer> getPartitions() {
        List<Integer> partitions = new ArrayList<Integer>();
        partitions.addAll(committer.getPidToOffsetInfo().keySet());
        return partitions;
    }

    public void forceCommitOffset() {
        Map<Integer, KafkaOffsetCommitter.CommitInfo> offsets = committer.forcePoll();
        if (!offsets.isEmpty()) {
            logger.info("force commit Offset to Kafka {}", offsets);
            doCommitOffset(offsets);
        }
    }

    public void commitOffset() {
        Map<Integer, KafkaOffsetCommitter.CommitInfo> offsets = committer.poll();
        if (!offsets.isEmpty()) {
            doCommitOffset(offsets);
        }
    }

    private void doCommitOffset(Map<Integer, KafkaOffsetCommitter.CommitInfo> CommitOffsets) {
        logger.debug("commit Offset to Kafka: {}", CommitOffsets);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (Entry<Integer, KafkaOffsetCommitter.CommitInfo> entry : CommitOffsets.entrySet()) {
            offsets.put(new TopicPartition(kafkaConsumerConf.getTopic(), entry.getKey()),
                    new OffsetAndMetadata(entry.getValue().getToOffset() + 1));
            PartitionInfo partitionInfo = partitionInfos.get(entry.getKey());
            if (partitionInfo != null) {
                partitionInfo.setCommitOffset(entry.getValue().getToOffset() + 1);
                partitionInfo.setLastCommitTimeMs(System.currentTimeMillis());
            }
        }
        try {
            consumer.commitSync(offsets);
        } catch (Exception e) {
            logger.error("commit Offset error {}", CommitOffsets, e);
        }
    }

    public List<WorkTask> fetchAndCreateTasks() {
        List<WorkTask> workTasks = new ArrayList<WorkTask>();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(kafkaConsumerConf.getPollTimeOut());

        Map<Integer, Integer> pidToSize = new HashMap<Integer, Integer>();
        Iterator<TopicPartition> iterator = records.partitions().iterator();
        for (; iterator.hasNext(); ) {
            TopicPartition partition = (TopicPartition) iterator.next();
            int partitionSize = records.records(partition).size();
            pidToSize.put(partition.partition(), partitionSize);
            if (partitionSize > 0) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                if (partitionInfos.containsKey(partition)) {
                    PartitionInfo partitionInfo = partitionInfos.get(partition);
                    TaskInfo taskInfo = new TaskInfo(createTaskId(partition.partition()), partition.partition(),
                            partitionRecords.get(0).offset(),
                            partitionRecords.get(partitionRecords.size() - 1).offset(), partitionRecords.size(),
                            System.currentTimeMillis(), 0, partitionInfo);
                    partitionInfo.setLastFetchTimeMs(taskInfo.getCreateTimestampMs());
                    partitionInfo.setNextOffset(taskInfo.getToOffset());

                    List<byte[]> rawRecords = new ArrayList<byte[]>();
                    for (ConsumerRecord<byte[], byte[]> partitionRecord : partitionRecords) {
                        rawRecords.add(partitionRecord.value());
                    }
                    workTasks.add(new WorkTask(taskInfo, rawRecords, this));
                }
            }
        }
        logger.debug("Read partition in this batch: {}", pidToSize);

        return workTasks;
    }

    protected abstract void start();

    public void wakeUp() {
        logger.info("wakeup kafka source");
        consumer.wakeup();
    }

    protected void close() {
        logger.info("close kafka source");
        committer.cancel();
        forceCommitOffset();
        consumer.close();
    }

    protected abstract void pause();

    protected abstract void resume();

    private long createTaskId(int pid) {
        long id = 0;

        if (pidToTaskId.containsKey(pid)) {
            id = pidToTaskId.get(pid) + 1;
            pidToTaskId.put(pid, id);
        } else {
            logger.error("Partition {} has not been registered", pid);
            pidToTaskId.put(pid, 0L);
        }

        return id;
    }
}
