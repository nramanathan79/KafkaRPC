package com.easyapp.kafka.rpc;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import com.easyapp.kafka.bean.RPCMessageMetadata;
import com.easyapp.kafka.producer.StringProducer;

public class StringRPC {
	private final Properties producerProperties;
	private final long timeoutMillis;

	public StringRPC(final Properties rpcProperties, final long timeoutMillis) {
		this.producerProperties = rpcProperties;
		this.timeoutMillis = timeoutMillis;
	}

	public Optional<String> rpcCall(final RPCMessageMetadata messageMetadata, final String requestMessage)
			throws Exception {
		List<String> responseList = new ArrayList<>();

		ServerSocket serverSocket = new ServerSocket(messageMetadata.getReplyPort());

		StringProducer producer = new StringProducer(producerProperties);
		producer.send(messageMetadata, requestMessage);
		producer.close();

		ExecutorService executor = Executors.newFixedThreadPool(messageMetadata.getNumberOfConsumers());
		List<Future<String>> threads = new ArrayList<>();

		IntStream.rangeClosed(1, messageMetadata.getNumberOfConsumers()).forEach(i -> {
			threads.add(executor.submit(new RPCSocketServer(serverSocket)));
		});

		threads.forEach(thread -> {
			try {
				responseList.add(thread.get(timeoutMillis, TimeUnit.MILLISECONDS));
			} catch (TimeoutException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});
		
		executor.shutdown();

		return responseList.size() >= messageMetadata.getNumberOfConsumers()
				? Optional.of("[" + String.join(", ", responseList) + "]") : Optional.empty();
	}
}
