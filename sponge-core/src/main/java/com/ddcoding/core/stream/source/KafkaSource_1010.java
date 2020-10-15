package com.ddcoding.core.stream.source;

import com.ddcoding.core.stream.partition.PartitionInfo;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KafkaSource_1010 extends KafkaSource {

	private final Logger logger = LoggerFactory.getLogger(KafkaSource_1010.class);

	public KafkaSource_1010(KafkaConsumerConf.ConsumerConf kafkaConsumerConf) {
		super(kafkaConsumerConf);
	}

	@Override
	protected void start() {
		List<String> topics = new ArrayList<String>();
		topics.add(kafkaConsumerConf.getTopic());
		consumer.subscribe(topics, new ConsumerRebalanceListener() {
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("assign new partitions:" + partitions);
				for (TopicPartition partition : partitions) {
					int pid = partition.partition();
					pidToTaskId.put(pid, -1L);
					committer.registerPartition(pid);

					OffsetAndMetadata committedOffset = consumer.committed(partition);
					if (committedOffset == null) {

						List<TopicPartition> partitionLst = new ArrayList<TopicPartition>();
						partitionLst.add(partition);
						consumer.seekToBeginning(partitionLst);
						logger.info("partition {} offset is unfounded, use beginning", partition.partition());
					} else {
						logger.info("partition {}, offset, {} is set in kafka already", partition,
								committedOffset.offset());
					}

					if (partitionInfos.containsKey(pid)) {
						PartitionInfo partitionInfo = partitionInfos.get(pid);
						partitionInfo.resetAttr();
						partitionInfo.setJoinTimeMs(System.currentTimeMillis());
						partitionInfo.setVersion(partitionInfo.getVersion() + 1);
					} else {
						PartitionInfo partitionInfo = new PartitionInfo(pid, System.currentTimeMillis());
						partitionInfos.put(pid, partitionInfo);
					}
				}
			}

			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				if (partitions.isEmpty()) {
					logger.info("no partition is revoked");
				} else {
					logger.info("revoke partitions: {}", partitions);
					for (TopicPartition partition : partitions) {
						forceCommitOffset();
						committer.unregisterPartition(partition.partition());
						partitionInfos.get(partition.partition()).setLeaveTimeMs(System.currentTimeMillis());
					}
				}
			}
		});
	}
	
	@Override
	protected void pause() {
		logger.info("pause consumer: {}", consumerID);
		consumer.pause(consumer.assignment());
	}

	@Override
	protected void resume() {
		logger.info("resume consumer: {}", consumerID);
		consumer.resume(consumer.assignment());
	}
}
