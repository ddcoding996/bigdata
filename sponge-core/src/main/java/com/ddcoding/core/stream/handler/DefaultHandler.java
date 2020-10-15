package com.ddcoding.core.stream.handler;

import com.ddcoding.core.SpongeServerConfig;
import com.ddcoding.core.extractor.Extractor;

public class DefaultHandler implements Handler {
	
	private Extractor extractor = Extractor.getInstance(SpongeServerConfig.getInstance().getConfig("extracor"));

	@Override
	public HandleContext Handle(HandleContext context) {
		
		
		return null;
	}

	@Override
	public void close() {
		
	}

}
