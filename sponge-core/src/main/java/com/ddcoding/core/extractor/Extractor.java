package com.ddcoding.core.extractor;

import com.typesafe.config.Config;

public class Extractor {

    private static Extractor extractor;

    private Extractor(Config config) {

    }

    private synchronized static void doSync(Config config) {
        if (null == extractor) {
            extractor = new Extractor(config);
        }
    }

    public static Extractor getInstance(Config config) {
        if (null == extractor) {
            doSync(config);
        }
        return extractor;
    }
}
