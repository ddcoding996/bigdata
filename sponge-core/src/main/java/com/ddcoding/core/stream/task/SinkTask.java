package com.ddcoding.core.stream.task;

import com.ddcoding.common.util.Executer;
import com.ddcoding.common.util.OperaUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SinkTask implements Task {

    public WorkTask getWorkTask() {
        return workTask;
    }

    public void setWorkTask(WorkTask workTask) {
        this.workTask = workTask;
    }

    private final Logger logger = LoggerFactory.getLogger(SinkTask.class);

    @Override
    public String toString() {
        return "SinkTask [taskinfo=" + taskinfo + "]";
    }

    public TaskInfo getTaskinfo() {
        return taskinfo;
    }

    public void setTaskinfo(TaskInfo taskinfo) {
        this.taskinfo = taskinfo;
    }

    public BulkRequest getBulkRequest() {
        return bulkRequest;
    }

    public void setBulkRequest(BulkRequest bulkRequest) {
        this.bulkRequest = bulkRequest;
    }

    public List<byte[]> getKafkaMessages() {
        return kafkaMessages;
    }

    public void setKafkaMessages(List<byte[]> kafkaMessages) {
        this.kafkaMessages = kafkaMessages;
    }

    private WorkTask workTask;
    private TaskInfo taskinfo;
    private BulkRequest bulkRequest;
    private List<byte[]> kafkaMessages;

    public SinkTask(TaskInfo taskinfo, BulkRequest bulkRequest, List<byte[]> kafkaMessages) {
        super();
        this.taskinfo = taskinfo;
        this.bulkRequest = bulkRequest;
        this.kafkaMessages = kafkaMessages;
    }

    public SinkTask(WorkTask workTask, BulkRequest bulkRequest, List<byte[]> kafkaMessages) {
        super();
        this.workTask = workTask;
        this.taskinfo = workTask.getTaskInfo();
        this.bulkRequest = bulkRequest;
        this.kafkaMessages = kafkaMessages;
    }

    public void run() {
        long duration = OperaUtil.getDuration(new Executer<SinkTask>() {
            @Override
            public void execute(SinkTask task) {
                workTask.getKafkaSource().getCommitter().commit(task.getTaskinfo());
            }
        }, this);

        logger.debug("this TaskDuration {}, {}", duration, this);
    }
}