package com.ddcoding.core.stream.handler;

import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.List;

public class HandleContext {
    private byte[] raw = null;
    private List<IndexRequest> esReqs = new ArrayList<IndexRequest>();
    private List<byte[]> kafkaMessages = new ArrayList<byte[]>();

    public HandleContext(byte[] raw) {
        this.raw = raw;
    }

    public byte[] getRaw() {
        return raw;
    }

    public void setRaw(byte[] raw) {
        this.raw = raw;
    }

    public List<IndexRequest> getEsReqs() {
        return esReqs;
    }

    public void setEsReqs(List<IndexRequest> esReqs) {
        this.esReqs = esReqs;
    }

    public List<byte[]> getKafkaMessages() {
        return kafkaMessages;
    }

    public void setKafkaMessages(List<byte[]> kafkaMessages) {
        this.kafkaMessages = kafkaMessages;
    }
}
