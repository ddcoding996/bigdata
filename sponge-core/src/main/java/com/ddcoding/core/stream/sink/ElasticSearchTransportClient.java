package com.ddcoding.core.stream.sink;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchTransportClient implements ElasticSearchClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchTransportClient.class);

    private static final int DEFAULT_PORT = 9300;
    private static final int SLEEP_TIME_MS = 1000;
    private InetSocketTransportAddress[] serverAddresses;
    private TransportClient client;

    public ElasticSearchTransportClient(Map<String, Integer> esNodes, String clusterName) throws UnknownHostException {
        configureHostnames(esNodes.keySet().toArray());
        Settings settings = Settings.builder().put("cluster.name", clusterName).put("transport.tcp.port", 0).build();

        TransportClient transportClient = (new TransportClient.Builder()).settings(settings).build();
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }

        if (client != null) {
            client.close();
        }
        client = transportClient;
    }

    private void configureHostnames(Object[] hostNames) throws UnknownHostException {
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = String.valueOf(hostNames[i]).trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : DEFAULT_PORT;
            serverAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(host), port);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public BulkRequest execute(BulkRequest bulkRequest) throws Exception {
        if (bulkRequest.numberOfActions() != 0) {
            BulkResponse bulkResponse;
            try {
                bulkResponse = client.bulk(bulkRequest).actionGet();
                if (bulkResponse.hasFailures()) {
                    return buildFailRetryRequest(bulkRequest, bulkResponse);
                } else {
                    return new BulkRequest();
                }
            } catch (IllegalStateException ex) {
                throw ex;
            } catch (Exception e) {
                logger.error("bulk index fail, error: {}", e.getMessage());
                List<String> indexNames = new ArrayList<String>();
                for (ActionRequest<?> actionRequest : bulkRequest.requests()) {
                    String indexName = ((IndexRequest) actionRequest).index();
                    if (!indexName.isEmpty()) {
                        indexNames.add(indexName);
                    }
                }
                indexNames.stream().distinct();
                waitForIndexesAvailable(indexNames);
            }
        }

        return bulkRequest;
    }

    @Override
    public ClusterHealthStatus getClusterStatus() {
        ClusterHealthStatus status;
        try {
            ClusterStatsResponse statsResponse = client.admin().cluster().prepareClusterStats()
                    .get(TimeValue.timeValueSeconds(5));
            status = statsResponse.getStatus();
        } catch (Exception e) {
            status = ClusterHealthStatus.RED;
        }

        return status;
    }

    private BulkRequest buildFailRetryRequest(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        BulkRequest failedRequests = new BulkRequest();
        bulkRequest.consistencyLevel(WriteConsistencyLevel.ONE);
        if (bulkResponse.hasFailures()) {
            BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
            for (int i = 0; i < bulkItemResponses.length; i++) {
                if (bulkItemResponses[i].isFailed()) {
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    if (indexRequest.id() == null) {
                        indexRequest.id(bulkItemResponses[i].getId());
                    }

                    RestStatus status = bulkItemResponses[i].getFailure().getStatus();
                    String failure = bulkItemResponses[i].getFailureMessage();

                    if (status != RestStatus.BAD_REQUEST && status != RestStatus.NOT_FOUND) {
                        if (!failure.startsWith("IndexClosedException")
                                && !failure.startsWith("IllegalArgumentException")) {
                            failedRequests.add(indexRequest);
                        }
                    }
                }
            }
        }

        return failedRequests;
    }

    private void waitForIndexesAvailable(List<String> indexeNames) {
        logger.info("wait until es is available");
        ClusterHealthStatus status = ClusterHealthStatus.RED;

        do {
            try {
                status = client.admin().cluster().prepareHealth((String[]) indexeNames.toArray())
                        .setWaitForYellowStatus().setTimeout(TimeValue.timeValueMillis(SLEEP_TIME_MS)).get()
                        .getStatus();
            } catch (IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                status = ClusterHealthStatus.RED;
                logger.warn("es index [{}] error, set status = red, {}", indexeNames, e);
            }

            if (status == ClusterHealthStatus.RED) {
                logger.warn("es index [{}] is unavailable now.", indexeNames);
                try {
                    Thread.sleep(SLEEP_TIME_MS);
                } catch (InterruptedException e) {
                }
            }
        } while (status == ClusterHealthStatus.RED);

        logger.info("es cluster status is ready now: {}", status);
    }
}