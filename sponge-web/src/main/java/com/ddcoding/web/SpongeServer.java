package com.ddcoding.web;

import com.ddcoding.common.util.ConfigHelper;
import com.ddcoding.core.SpongeServerConfig;
import com.ddcoding.core.server.handler.GetRulesHandler;
import com.ddcoding.core.server.handler.VerifyRulesHandler;
import com.ddcoding.core.stream.Stream;
import com.ddcoding.core.stream.StreamConfig;
import com.typesafe.config.Config;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Desc
 * @Date 2020/1/15 15:50
 * @Author ddcoding
 */
public class SpongeServer {
    private static final Logger logger = LoggerFactory.getLogger(SpongeServer.class);
    private static final HandlerList list = new HandlerList();
    private static final GetRulesHandler getRulesHandler = new GetRulesHandler();
    private static final VerifyRulesHandler verifyRulesHandler = new VerifyRulesHandler();

    public static void main(String[] args) throws Exception {

        Config config = SpongeServerConfig.getInstance();
        if (config.isEmpty()) {
            logger.error("config file is not existed!");
            System.exit(-1);
        }

        // DbUtil.init(config.getConfig("mysql"));
        StreamConfig streamConfig = StreamConfig.getInstance(config.getConfig("stream"));
        final Stream stream = new Stream(streamConfig);
        stream.start();

        // List<Extractrule> lst = Extractrule.findAll(AutoSession.readOnly());
        Server server = new Server(ConfigHelper.getOrElse(config, "service.port", 9091));
        list.setHandlers(new Handler[]{getRulesHandler, verifyRulesHandler});
        server.setHandler(list);
        server.start();
        server.join();

        Runtime.getRuntime().addShutdownHook(new Thread("External stream shutdown thread") {
            @Override
            public void run() {
                try {
                    stream.shutdown();
                } catch (Exception e) {
                    logger.info("stream shutdown failed. e: ", e);
                }
                logger.info("stream is closed.");
            }
        });

        stream.awaitShutdown();
    }
}
