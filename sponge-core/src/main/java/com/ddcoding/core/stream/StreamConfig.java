package com.ddcoding.core.stream;

import com.ddcoding.common.util.ConfigHelper;
import com.ddcoding.common.util.OperaUtil;
import com.ddcoding.core.stream.source.KafkaConsumerConf;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamConfig {

    private static StreamConfig streamConfig = null;
    private int sinkCount = OperaUtil.getCpuNum();
    private int workCount = OperaUtil.getCpuNum();
    private double workFactor = 0.5;
    private double sinkFactor = 1.0;
    private int kafkaSourceParallelNum = 1;
    private KafkaConsumerConf.ConsumerConf kafkaConsumerConf = null;
    private String esClusterName = "";
    private Map<String, Integer> esNodes = new HashMap<String, Integer>();
    private long beforeDurationMs = 0;
    private long afterDurationMs = 0;
    private int indexPeriod = 0;
    private String indexPeriodTimeUnit = "";
    private long indexPeriodMs = 0;
    private Boolean esIndexUseAutoId = false;
    private Config extractConfig = null;
    private String zkAddr = "";

    public int getSinkCount() {
        return sinkCount;
    }

    public void setSinkCount(int sinkCount) {
        this.sinkCount = sinkCount;
    }

    public int getWorkCount() {
        return workCount;
    }

    public void setWorkCount(int workCount) {
        this.workCount = workCount;
    }

    public double getWorkFactor() {
        return this.workFactor;
    }

    public void setWorkFactor(double workFactor) {
        this.workFactor = workFactor;
    }

    public double getSinkFactor() {
        return sinkFactor;
    }

    public void setSinkFactor(double sinkFactor) {
        this.sinkFactor = sinkFactor;
    }

    public int getKafkaSourceParallelNum() {
        return kafkaSourceParallelNum;
    }

    public void setKafkaSourceParallelNum(int kafkaSourceParallelNum) {
        this.kafkaSourceParallelNum = kafkaSourceParallelNum;
    }

    public String getEsClusterName() {
        return esClusterName;
    }

    public void setEsClusterName(String esClusterName) {
        this.esClusterName = esClusterName;
    }

    public Map<String, Integer> getEsNodes() {
        return esNodes;
    }

    public void setEsNodes(Map<String, Integer> esNodes) {
        this.esNodes = esNodes;
    }

    public long getBeforeDurationMs() {
        return beforeDurationMs;
    }

    public void setBeforeDurationMs(long beforeDurationMs) {
        this.beforeDurationMs = beforeDurationMs;
    }

    public long getAfterDurationMs() {
        return afterDurationMs;
    }

    public void setAfterDurationMs(long afterDurationMs) {
        this.afterDurationMs = afterDurationMs;
    }

    public int getIndexPeriod() {
        return indexPeriod;
    }

    public void setIndexPeriod(int indexPeriod) {
        this.indexPeriod = indexPeriod;
    }

    public String getIndexPeriodTimeUnit() {
        return indexPeriodTimeUnit;
    }

    public void setIndexPeriodTimeUnit(String indexPeriodTimeUnit) {
        this.indexPeriodTimeUnit = indexPeriodTimeUnit;
    }

    public long getIndexPeriodMs() {
        return indexPeriodMs;
    }

    public void setIndexPeriodMs(long indexPeriodMs) {
        this.indexPeriodMs = indexPeriodMs;
    }

    public Boolean getEsIndexUseAutoId() {
        return esIndexUseAutoId;
    }

    public void setEsIndexUseAutoId(Boolean esIndexUseAutoId) {
        this.esIndexUseAutoId = esIndexUseAutoId;
    }

    public Config getExtractConfig() {
        return extractConfig;
    }

    public void setExtractConfig(Config extractConfig) {
        this.extractConfig = extractConfig;
    }

    public String getZkAddr() {
        return zkAddr;
    }

    public void setZkAddr(String zkAddr) {
        this.zkAddr = zkAddr;
    }

    public KafkaConsumerConf.ConsumerConf getKafkaConsumerConf() {
        return this.kafkaConsumerConf;
    }

    public void setKafkaConsumerConf(KafkaConsumerConf.ConsumerConf kafkaConsumerConf) {
        this.kafkaConsumerConf = kafkaConsumerConf;
    }

    private StreamConfig(Config config) throws Exception {
        sinkCount = ConfigHelper.getOrElse(config, "sink_count", sinkCount);
        workCount = ConfigHelper.getOrElse(config, "work_count", workCount);
        workFactor = ConfigHelper.getOrElse(config, "work_factor", workFactor);
        sinkFactor = ConfigHelper.getOrElse(config, "sink_factor", sinkFactor);
        kafkaConsumerConf = KafkaConsumerConf.getInstance(config.getConfig("kafka_source"));
        kafkaSourceParallelNum = ConfigHelper.getOrElse(config, "kafka_source.parallel_num", kafkaSourceParallelNum);
        zkAddr = ConfigHelper.getOrElse(config, "kafka_source.zk_addr", zkAddr);
        esClusterName = ConfigHelper.getOrElse(config, "es_sink.cluster_name", esClusterName);
        esNodes = ConfigHelper.getMapOrElse(config, "es_sink.nodes", esNodes);
        Config indexConfig = config.getConfig("es_sink.index");
        beforeDurationMs = indexConfig.getDuration("before_duration", TimeUnit.MILLISECONDS);
        afterDurationMs = indexConfig.getDuration("after_duration", TimeUnit.MILLISECONDS);
        esIndexUseAutoId = ConfigHelper.getOrElse(indexConfig, "use_auto_id", esIndexUseAutoId);
        Pattern pattern = Pattern.compile("(\\d+)\\s*(h|d)");
        Matcher matcher = pattern.matcher(indexConfig.getString("index_period"));
        boolean rs = matcher.matches();
        if (rs) {
            indexPeriod = Integer.parseInt(matcher.group(1));
            indexPeriodTimeUnit = matcher.group(1);
            indexPeriodMs = indexConfig.getDuration("index_period", TimeUnit.MILLISECONDS);
        }
        extractConfig = config.getConfig("extract");
    }

    private synchronized static void doSync(Config config) throws Exception {
        if (null == streamConfig) {
            streamConfig = new StreamConfig(config);
        }
    }

    public static StreamConfig getInstance(Config config) throws Exception {
        if (null == streamConfig) {
            doSync(config);
        }
        return streamConfig;
    }
}
