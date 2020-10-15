package com.ddcoding.core.stream.source;

import com.ddcoding.core.stream.task.TaskInfo;
import com.ddcoding.core.stream.task.TaskInfoComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaOffsetCommitter {
    public Map<Integer, OffsetInfo> getPidToOffsetInfo() {
        return pidToOffsetInfo;
    }

    public void setPidToOffsetInfo(Map<Integer, OffsetInfo> pidToOffsetInfo) {
        this.pidToOffsetInfo = pidToOffsetInfo;
    }

    private final Logger logger = LoggerFactory.getLogger(KafkaOffsetCommitter.class);

    private static final int COMMIT_PERIOD = 5;

    private Map<Integer, OffsetInfo> pidToOffsetInfo = new ConcurrentHashMap<Integer, OffsetInfo>();
    private Queue<Map<Integer, CommitInfo>> commitQueue = new ArrayBlockingQueue<Map<Integer, CommitInfo>>(1);
    private Timer timer = new Timer(true);

    public KafkaOffsetCommitter() {
        timer.schedule(new java.util.TimerTask() {
            public void run() {
                Map<Integer, CommitInfo> commitInfos = getCommitInfos();
                if (!commitInfos.isEmpty()) {
                    logger.debug("Commit commitInfos to commitQueue {}", commitInfos);
                    commitQueue.add(commitInfos);
                } else {
                    logger.debug("Commit commitInfos to commitQueue, commitInfos == empty, " + "pidToOffset:{},",
                            pidToOffsetInfo);
                }
            }
        }, 0, COMMIT_PERIOD * 1000);
    }

    private Map<Integer, CommitInfo> getCommitInfos() {
        Map<Integer, CommitInfo> pidToCommitInfo = new HashMap<Integer, CommitInfo>();
        for (Entry<Integer, OffsetInfo> entry : pidToOffsetInfo.entrySet()) {
            int pid = entry.getKey().intValue();
            CommitInfo commitInfo = entry.getValue().getCommitInfo();
            if (commitInfo != null) {
                pidToCommitInfo.put(pid, commitInfo);
            }
        }

        return pidToCommitInfo;
    }

    public void commit(TaskInfo taskInfo) {
        OffsetInfo offsetInfo = pidToOffsetInfo.get(taskInfo.getPartitionId());
        if (offsetInfo == null) {
            logger.warn("Partition {} is unregistered, task:{}", taskInfo.getPartitionId(), taskInfo);
        } else {
            offsetInfo.add(taskInfo);
        }
    }

    public void cancel() {
        logger.info("cancel scheduler for merge commit offset");
        timer.cancel();
    }

    public Map<Integer, CommitInfo> poll() {
        return commitQueue.poll();
    }

    public Map<Integer, CommitInfo> forcePoll() {
        Map<Integer, CommitInfo> forceMap = new HashMap<Integer, CommitInfo>();
        Map<Integer, CommitInfo> curMap = poll();
        forceMap = getCommitInfos();

        for (Entry<Integer, CommitInfo> entry : curMap.entrySet()) {
            if (!forceMap.keySet().contains(entry.getKey())) {
                forceMap.put(entry.getKey(), entry.getValue());
            }
        }

        return forceMap;
    }

    public void registerPartition(int pid) {
        logger.info("Register partition to OffsetCommitter pid:{}", pid);
        pidToOffsetInfo.put(pid, new OffsetInfo());
    }

    public void unregisterPartition(int pid) {
        logger.info("UnRegister partition from OffsetCommitter pid:{}", pid);
        pidToOffsetInfo.remove(pid);
    }

    public class CommitInfo {
        @Override
        public String toString() {
            return "CommitInfo [taskIds=" + taskIds + ", fromOffset=" + fromOffset + ", toOffset=" + toOffset
                    + ", size=" + size + "]";
        }

        private List<Long> taskIds;
        private long fromOffset;
        private long toOffset;
        private long size;

        public CommitInfo(List<Long> taskId, long fromOffset, long toOffset, long size) {
            this.taskIds = taskId;
            this.fromOffset = fromOffset;
            this.toOffset = toOffset;
            this.size = size;
        }

        public List<Long> getTaskIds() {
            return taskIds;
        }

        public void setTaskIds(List<Long> taskIds) {
            this.taskIds = taskIds;
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
    }

    private class OffsetInfo {
        private final Logger logger = LoggerFactory.getLogger(OffsetInfo.class);

        TaskInfoComparator comparator = new TaskInfoComparator();
        private TreeSet<TaskInfo> taskInfoSet = new TreeSet<TaskInfo>(comparator);
        private long createTimeMs = System.currentTimeMillis();
        private long lastSubmittedTaskId = -1L;

        @Override
        public String toString() {
            return "OffsetInfo [taskInfoSet=" + taskInfoSet + ", lastSubmittedTaskId=" + lastSubmittedTaskId + "]";
        }

        public synchronized void add(TaskInfo task) {
            if (task.getCreateTimestampMs() >= createTimeMs) {
                taskInfoSet.add(task);
            } else {
                logger.warn("TaskVersion != version [{}], task: {}", createTimeMs, task);
            }
        }

        public CommitInfo getCommitInfo() {
            List<TaskInfo> taskInfoList = new ArrayList<TaskInfo>();
            synchronized (this) {
                for (TaskInfo taskInfo : taskInfoList) {
                    if (taskInfo.getTaskId() == lastSubmittedTaskId + 1) {
                        taskInfoList.add(taskInfo);
                        lastSubmittedTaskId = taskInfo.getTaskId();
                        taskInfoSet.remove(taskInfo);
                    }
                }
            }

            logger.debug("get CommitInfo: {}, \t taskInfoSet: {}", taskInfoList, taskInfoSet);

            if (taskInfoList.isEmpty()) {
                return null;
            } else {
                List<Long> taskIds = new ArrayList<Long>();
                long size = 0;
                for (TaskInfo taskInfo : taskInfoList) {
                    taskIds.add(taskInfo.getTaskId());
                    size += taskInfo.getSize();
                }
                return new CommitInfo(taskIds, taskInfoList.get(0).getFromOffset(),
                        taskInfoList.get(taskInfoList.size() - 1).getToOffset(), size);
            }
        }
    }
}
