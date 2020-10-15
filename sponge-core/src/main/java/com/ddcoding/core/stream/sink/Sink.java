package com.ddcoding.core.stream.sink;


import com.ddcoding.core.stream.task.SinkTask;

public interface Sink {
    void process(SinkTask task) throws Exception;

    void close();
}
