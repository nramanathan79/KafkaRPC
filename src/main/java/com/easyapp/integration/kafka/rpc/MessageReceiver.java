package com.easyapp.integration.kafka.rpc;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class MessageReceiver implements Callable<String> {
	private final String key;
	private final RPCService rpcService;
	private final long timeoutMillis;

	public MessageReceiver(final String key, final RPCService rpcService, final long timeoutMillis) {
		this.key = key;
		this.rpcService = rpcService;
		this.timeoutMillis = timeoutMillis;
	}

	@Override
	public String call() throws InterruptedException {
		return rpcService.getQueue(key).poll(timeoutMillis, TimeUnit.MILLISECONDS);
	}
}
