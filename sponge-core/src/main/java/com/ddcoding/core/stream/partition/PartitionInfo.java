package com.ddcoding.core.stream.partition;

public class PartitionInfo {
    private int pid;
    private int version = 0;
    private long nextOffset = -1;
    private long commitOffset = -1;
    private long lastFetchTimeMs = -1;
    private long lastCommitTimeMs = -1;
    private long joinTimeMs = -1;
    volatile private long leaveTimeMs = -1;

    public PartitionInfo(int pid, long joinTimeMs) {
        this.pid = pid;
        this.joinTimeMs = joinTimeMs;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public long getLastFetchTimeMs() {
        return lastFetchTimeMs;
    }

    public void setLastFetchTimeMs(long lastFetchTimeMs) {
        this.lastFetchTimeMs = lastFetchTimeMs;
    }

    public long getLastCommitTimeMs() {
        return lastCommitTimeMs;
    }

    public void setLastCommitTimeMs(long lastCommitTimeMs) {
        this.lastCommitTimeMs = lastCommitTimeMs;
    }

    public long getJoinTimeMs() {
        return joinTimeMs;
    }

    public void setJoinTimeMs(long joinTimeMs) {
        this.joinTimeMs = joinTimeMs;
    }

    public long getLeaveTimeMs() {
        return leaveTimeMs;
    }

    public void setLeaveTimeMs(long leaveTimeMs) {
        this.leaveTimeMs = leaveTimeMs;
    }

    @Override
    public String toString() {
        return "PartitionInfo [pid=" + pid + ", version=" + version + ", nextOffset=" + nextOffset + ", commitOffset="
                + commitOffset + ", lastFetchTimeMs=" + lastFetchTimeMs + ", lastCommitTimeMs=" + lastCommitTimeMs
                + ", joinTimeMs=" + joinTimeMs + ", leaveTimeMs=" + leaveTimeMs + "]";
    }

    public Boolean isAlive() {
        return leaveTimeMs == -1;
    }

    public void resetAttr() {
        nextOffset = -1;
        commitOffset = -1;
        lastFetchTimeMs = -1;
        lastCommitTimeMs = -1;
        joinTimeMs = -1;
        leaveTimeMs = -1;
    }
}