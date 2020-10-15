package com.ddcoding.core.stream.source;

import com.typesafe.config.Config;
import java.util.Properties;

public class KafkaConsumerConf {

    private static ConsumerConf kafkaConsumerConf;

    private KafkaConsumerConf() {
    }

    private synchronized static void doSync(Config config) {
        if (null == kafkaConsumerConf) {
            Properties props = new Properties();

            props.put("bootstrap.servers", config.getStringList("bootstrap_servers"));
            props.put("group.id", config.getString("group_id"));
            props.put("max.partition.fetch.bytes", config.getString("max_partition_fetch_bytes"));
            props.put("max.poll.records", config.getLong("max_poll_records"));
            props.put("enable.auto.commit", "false");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            String topic = config.getString("topic");
            long pollTimeout = config.getLong("poll_timeout_ms");

            kafkaConsumerConf = new ConsumerConf(props, topic, pollTimeout);
        }
    }

    public static ConsumerConf getInstance(Config config) {
        if (null == kafkaConsumerConf) {
            doSync(config);
        }
        return kafkaConsumerConf;
    }

    public static class ConsumerConf {
        private Properties properties;
        private String topic;
        private Long pollTimeout;

        public ConsumerConf(Properties properties, String topic, Long pollTimeout) {
            this.properties = properties;
            this.topic = topic;
            this.pollTimeout = pollTimeout;
        }

        public Properties getProperties() {
            return this.properties;
        }

        public void setProperties(Properties properties) {
            this.properties = properties;
        }

        public String getTopic() {
            return this.topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Long getPollTimeOut() {
            return this.pollTimeout;
        }

        public void setPollTimeout(Long pollTimeout) {
            this.pollTimeout = pollTimeout;
        }
    }
}
