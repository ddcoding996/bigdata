package com.ddcoding.core.stream.handler;

public interface Handler {

    HandleContext Handle(HandleContext context);

    void close();
}
