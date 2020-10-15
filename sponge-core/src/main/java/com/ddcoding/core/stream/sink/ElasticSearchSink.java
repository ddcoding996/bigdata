package com.ddcoding.core.stream.sink;

import com.ddcoding.core.SpongeServerConfig;
import com.ddcoding.core.stream.StreamConfig;
import com.ddcoding.core.stream.task.SinkTask;
import org.elasticsearch.action.bulk.BulkRequest;

import java.net.UnknownHostException;

public class ElasticSearchSink implements Sink {

	private static final int FAIL_RETRY_TIMES = 10;
	private static final int RETRY_SLEEP_MS = 1000;
	
	private ElasticSearchClient client;
	
	public ElasticSearchSink(StreamConfig config) {
		try {
			client = new ElasticSearchTransportClient(config.getEsNodes(), config.getEsClusterName());
		} catch (UnknownHostException e) {
			if (e != null) {
                client.close();
            }
		}
	}
	
	@Override
	public void process(SinkTask task) throws Exception {
		BulkRequest bulkRequest = task.getBulkRequest();
		if (bulkRequest.numberOfActions() != 0) {
			Boolean isSuccess = false;
			int tryTimes = 0;
			while (!isSuccess && tryTimes < FAIL_RETRY_TIMES && bulkRequest.numberOfActions() != 0) {
				BulkRequest retryRequests = client.execute(bulkRequest);
				if (retryRequests.numberOfActions() == 0) {
					isSuccess = true;
				} else {
					bulkRequest = retryRequests;
				    Thread.sleep(RETRY_SLEEP_MS);
				}
				tryTimes += 1;
			}
		}
	}
	
	@Override
	public void close() {
		client.close();
	}
}
