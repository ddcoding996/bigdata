package com.ddcoding.core.extractor.handler;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;

public class GrokHandler extends Handler {

    public GrokHandler(String fieldSrcPath) {
        super(fieldSrcPath);
    }

    public static void main(String[] args) {
        String grokJsonStr = "{\"timestamp\":\"(\\\\d{4}-\\\\d{2}-\\\\d{2}\\\\s\\\\d{2}:\\\\d{2}:\\\\d{2})\", \"geo\":{\"province\":\"(\\\\S+)\",\"city\":\"(\\\\S+)\"}, \"hostname\":\"(\\\\d+.\\\\d+.\\\\d+.\\\\d+)\"}";
        Map<String, Object> map = new Gson().fromJson(grokJsonStr, Map.class);
        for (Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
                for (Entry en : valueMap.entrySet()) {
                    System.out.println("key:" + en.getKey() + ",value:" + en.getValue());
                }
            } else {
                System.out.println("key:" + entry.getKey() + ",value:" + entry.getValue());
            }
        }
    }
}
