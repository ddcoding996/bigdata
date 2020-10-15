package com.ddcoding.common.util;

@FunctionalInterface
public interface Executer<T> {
    void execute(T t);
}
