package com.ddcoding.common.util;

public class OperaUtil {

    public static int getCpuNum() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static <T> long getDuration(Executer<T> executer, T t) {
        long startTime = System.nanoTime();
        executer.execute(t);

        return (System.nanoTime() - startTime) / 1000000;
    }
}
