package com.ddcoding.core.server.handler;

import com.alibaba.fastjson.JSONObject;
import com.ddcoding.common.util.RequestToJson;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class VerifyRulesHandler extends AbstractHandler {
    private static final Logger logger = LoggerFactory
            .getLogger(VerifyRulesHandler.class);

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        JSONObject jsonObj = new JSONObject();
        String message = "";
        if (target.startsWith("/api/verify_rules")) {
            baseRequest.setHandled(true);
            response.setContentType("text/html;charset=utf-8");

            JSONObject reqJson = RequestToJson.requestToJson(request);
            if (reqJson == null) {
                jsonObj.put("status", -1);
                jsonObj.put("message", "bad request.");
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().println(jsonObj.toString());
                return;
            }
            //...........等待补充

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(jsonObj.toString());
        } else {
            message = "check your url, no handler will process your request.";
            jsonObj.put("status", -1);
            jsonObj.put("message", message);
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().println(jsonObj.toString());
        }
    }
}
