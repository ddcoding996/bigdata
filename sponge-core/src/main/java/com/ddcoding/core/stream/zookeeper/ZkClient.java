package com.ddcoding.core.stream.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClient {

    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private final static int DEFAULT_SESSION_TIMEOUT_MS = 60000;
    private final static int DEFAULT_CONNECTION_TIMEOUT_MS = 15000;
    private final static int NTIMES = 5;
    private final static int SLEEPMS_BETWEEN_TRY = 2000;

    private static ZkClient zkClient = null;
    private final CuratorFramework client;

    private ZkClient(String address, int sessionTimeoutMs, int connectTimeoutMs) {
        this.client = CuratorFrameworkFactory.newClient(address, sessionTimeoutMs, connectTimeoutMs,
                new RetryNTimes(ZkClient.NTIMES, ZkClient.SLEEPMS_BETWEEN_TRY));
    }

    public static ZkClient getClient(String address) {
        if (zkClient == null) {
            zkClient = new ZkClient(address, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
        }

        return zkClient;
    }

    public void start() {
        client.start();
    }

    public void close() {
        client.close();
    }

    public long getOffset(String topic, int partition) {
        long offset;
        String path = "/consumers/streaming-consumer-group/offsets/" + topic + "/" + partition;
        Stat stat = null;
        try {
            stat = client.checkExists().forPath(path);
        } catch (Exception e1) {
        }
        if (stat == null) {
            logger.warn("client for {} is not set", path);
            offset = 0;
        } else {
            try {
                offset = Long.parseLong(new String(client.getData().forPath(path)));
            } catch (Exception e) {
                logger.error("Get From {} error{}", path, e);
                offset = 0;
            }
        }
        return offset;
    }
}
