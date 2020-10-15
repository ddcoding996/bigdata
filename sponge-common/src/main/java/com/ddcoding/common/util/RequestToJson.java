package com.ddcoding.common.util;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class RequestToJson {

    private static final Logger logger = LoggerFactory.getLogger(RequestToJson.class);

    public static byte[] readBytes(InputStream is, int contentLen) {
        if (contentLen > 0) {
            int readLen = 0;
            int readLengthThisTime = 0;
            byte[] message = new byte[contentLen];
            try {
                while (readLen != contentLen) {
                    readLengthThisTime = is.read(message, readLen, contentLen - readLen);
                    if (readLengthThisTime == -1) {
                        break;
                    }
                    readLen += readLengthThisTime;
                }
                return message;
            } catch (IOException e) {
            }
        }

        return new byte[]{};
    }

    public static JSONObject readJson(byte[] reqBodyBytes) {

        String res = new String(reqBodyBytes);
        JSONObject reqJson = new JSONObject();
        try {
            reqJson = JSONObject.parseObject(res);
        } catch (JSONException e) {
            logger.error("request is not json format. request: " + res);
            reqJson = null;
        }

        return reqJson;
    }

    public static JSONObject requestToJson(HttpServletRequest request) {

        JSONObject reqJson = null;

        try {
            request.setCharacterEncoding("UTF-8");
        } catch (UnsupportedEncodingException e1) {
            reqJson = null;
            return reqJson;
        }

        int size = request.getContentLength();
        InputStream is = null;
        try {
            is = request.getInputStream();
        } catch (IOException e) {
            reqJson = null;
            return reqJson;
        }

        byte[] reqBodyBytes = readBytes(is, size);
        if (reqBodyBytes.length > 0) {
            reqJson = readJson(reqBodyBytes);
        }
        return reqJson;
    }

}
