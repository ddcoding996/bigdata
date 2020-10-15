package com.ddcoding.core.extractor.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handler {

	private static final Logger logger = LoggerFactory.getLogger(Handler.class);

	private String fieldSrcPath;

	public Handler(String fieldSrcPath) {
		this.fieldSrcPath = fieldSrcPath;
	}
}
