package com.ddcoding.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * @Desc
 * @Date 2020/1/15 15:51
 * @Author ddcoding
 */
public class SpongeServerConfig {

    private static String path = SpongeServerConfig.class.getClassLoader().getResource("config/sponge.conf").getPath();
    private static Config config;

    private SpongeServerConfig() {
    }

    private synchronized static void doSync() {
        if (null == config) {
            File file = new File(path);
            config = ConfigFactory.parseFile(file).resolve();
        }
    }

    public static Config getInstance() {
        if (null == config) {
            doSync();
        }
        return config;
    }
}