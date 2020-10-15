package com.ddcoding.core.stream.sink;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

public interface ElasticSearchClient {
    void close();

    ClusterHealthStatus getClusterStatus();

    BulkRequest execute(BulkRequest bulkRequest) throws Exception;
}
