package com.easyapp.integration.kafka.rpc;

import java.util.concurrent.Callable;

public class MessageReceiver implements Callable<String> {
	private final String key;
	private final RPCService rpcService;

	public MessageReceiver(final String key, final RPCService rpcService) {
		this.key = key;
		this.rpcService = rpcService;
	}

	@Override
	public String call() throws InterruptedException {
		return rpcService.getQueue(key).take();
	}
}
