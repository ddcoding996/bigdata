package com.ddcoding.core.extractor.extract;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Event {

    private static String UNHANDLELABEL = "not_handle";

    private String domain;
    private String appname;
    private String hostname;
    private String token;
    private String identifiedID;
    private String rawData;
    private String cacheKey;
    private String group;
    private long timestamp;
    private boolean needHandle = true;

    private byte[] outMessage = null;
    private List<String> tags = new ArrayList<String>();
    private Map<String, Object> fields = new HashMap<String, Object>();

    @Override
    public String toString() {
        return "Event [domain=" + domain + ", appname=" + appname + ", hostname=" + hostname + ", token=" + token
                + ", identifiedID=" + identifiedID + ", rawData=" + rawData + ", cacheKey=" + cacheKey + ", group="
                + group + ", timestamp=" + timestamp + ", needHandle=" + needHandle + ", outMessage="
                + Arrays.toString(outMessage) + ", tags=" + tags + ", fields=" + fields + "]";
    }

    public Event() {
    }

    public Event(String sourceData) {
        JSONObject sourceJson = JSON.parseObject(sourceData);
        domain = sourceJson.getString("domain");
        appname = sourceJson.getString("appname");
        hostname = sourceJson.getString("hostname");
        token = sourceJson.getString("token");
        identifiedID = sourceJson.getString("identified_id");
        rawData = sourceJson.getString("raw_data");
        timestamp = sourceJson.getLongValue("timestamp");
        String sourceTags = sourceJson.getString("tags");
        if (sourceTags != null) {
            String[] splitTags = sourceTags.split(",");
            for (String tag : splitTags) {
                if (tag.equals(UNHANDLELABEL)) {
                    needHandle = false;
                }
                tags.add(tag);
            }
        }
    }

    private String createCacheKey() {
        StringBuilder cacheKey = new StringBuilder();
        cacheKey.append(domain).append("@").append(appname).append("@");
        for (String tag : tags) {
            if (tag.equals(tags.get(tags.size() - 1))) {
                cacheKey.append(tag);
            } else {
                cacheKey.append(tag).append(",");
            }
        }

        return cacheKey.toString();
    }

    public Object parseEvent(List<String> sourcePath) {
        if (sourcePath.isEmpty()) {
            return null;
        } else {
            return null;
        }
    }
}
