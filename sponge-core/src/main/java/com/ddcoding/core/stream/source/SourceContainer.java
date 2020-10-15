package com.ddcoding.core.stream.source;

import com.ddcoding.core.stream.manager.WorkerManager;

public abstract class SourceContainer {

    private final WorkerManager workerManager;

    public SourceContainer(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    public WorkerManager getWorkerManager() {
        return workerManager;
    }

    protected abstract void start();

    protected abstract void shutdown() throws InterruptedException;
}
