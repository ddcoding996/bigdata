package com.ddcoding.core.stream.task;

import com.ddcoding.common.util.Executer;
import com.ddcoding.common.util.OperaUtil;
import com.ddcoding.core.stream.handler.DefaultHandler;
import com.ddcoding.core.stream.handler.HandleContext;
import com.ddcoding.core.stream.handler.Handler;
import com.ddcoding.core.stream.source.KafkaSource;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WorkTask implements Task {

    private final Logger logger = LoggerFactory.getLogger(WorkTask.class);

    private final Handler handler = new DefaultHandler();

    @Override
    public String toString() {
        return "WorkTask [taskInfo=" + taskInfo + "]";
    }

    private TaskInfo taskInfo;
    private List<byte[]> records;
    private KafkaSource kafkaSource;

    public WorkTask(TaskInfo taskInfo, List<byte[]> records, KafkaSource kafkaSource) {
        this.taskInfo = taskInfo;
        this.records = records;
        this.kafkaSource = kafkaSource;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    public List<byte[]> getRecords() {
        return records;
    }

    public void setRecords(List<byte[]> records) {
        this.records = records;
    }

    public KafkaSource getKafkaSource() {
        return kafkaSource;
    }

    public void setKafkaSource(KafkaSource kafkaSource) {
        this.kafkaSource = kafkaSource;
    }

    public void run() {
        long duration = OperaUtil.getDuration(new Executer<WorkTask>() {
            @Override
            public void execute(WorkTask task) {
                List<byte[]> kafkaMessages = new ArrayList<byte[]>();
                BulkRequest bulkRequest = new BulkRequest();
                bulkRequest.consistencyLevel(WriteConsistencyLevel.ONE);

                for (byte[] record : task.getRecords()) {
                    HandleContext context = handler.Handle(new HandleContext(record));
                    for (IndexRequest indexRequest : context.getEsReqs()) {
                        bulkRequest.add(indexRequest);
                    }

                    if (!context.getKafkaMessages().isEmpty()) {
                        kafkaMessages.addAll(context.getKafkaMessages());
                    }
                }
            }
        }, this);

        logger.debug("this TaskDuration {}, {}", duration, this);
    }
}
