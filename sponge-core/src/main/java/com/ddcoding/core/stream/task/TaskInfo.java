package com.ddcoding.core.stream.task;


import com.ddcoding.core.stream.partition.PartitionInfo;

public class TaskInfo {
    public TaskInfo(long taskId, int partitionId, long fromOffset, long toOffset, long size, long createTimestampMs,
                    long duration, PartitionInfo partitionInfo) {
        this.taskId = taskId;
        this.partitionId = partitionId;
        this.fromOffset = fromOffset;
        this.toOffset = toOffset;
        this.size = size;
        this.createTimestampMs = createTimestampMs;
        this.duration = duration;
        this.partitionInfo = partitionInfo;
    }

    private long taskId;
    private int partitionId;
    private long fromOffset;
    private long toOffset;
    private long size;
    private long createTimestampMs;
    private long duration;
    private PartitionInfo partitionInfo = null;

    @Override
    public String toString() {
        return "TaskInfo [taskId=" + taskId + ", partitionId=" + partitionId + ", fromOffset=" + fromOffset
                + ", toOffset=" + toOffset + ", size=" + size + ", createTimestampMs=" + createTimestampMs
                + ", duration=" + duration + "]";
    }

    public Boolean isAlive() {
        if (partitionInfo == null) {
            return false;
        } else {
            return partitionInfo.isAlive() && createTimestampMs >= partitionInfo.getJoinTimeMs();
        }
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public void setFromOffset(long fromOffset) {
        this.fromOffset = fromOffset;
    }

    public long getToOffset() {
        return toOffset;
    }

    public void setToOffset(long toOffset) {
        this.toOffset = toOffset;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getCreateTimestampMs() {
        return createTimestampMs;
    }

    public void setCreateTimestampMs(long createTimestampMs) {
        this.createTimestampMs = createTimestampMs;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }
}