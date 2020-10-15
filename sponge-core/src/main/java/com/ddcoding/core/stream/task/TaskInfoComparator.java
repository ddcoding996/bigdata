package com.ddcoding.core.stream.task;

import java.util.Comparator;

public class TaskInfoComparator implements Comparator<TaskInfo> {

    public int compare(TaskInfo t1, TaskInfo t2) {
        if (t1 == t2)
            return 0;
        if (t1 != null && t2 == null)
            return 1;
        else if (t1 == null && t2 != null)
            return -1;
        if (t1.getTaskId() > t2.getTaskId())
            return 1;
        else if (t1.getTaskId() < t2.getTaskId())
            return -1;
        else
            return 0;
    }
}