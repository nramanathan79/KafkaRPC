package com.easyapp.kafka.rpc;

import java.io.IOException;
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
			throws IOException {
		List<String> responseList = new ArrayList<>();

		// Create a server socket to listen for response
		ServerSocket serverSocket = new ServerSocket(messageMetadata.getReplyPort());

		// Create threads to accept connections from responding RPC consumers
		ExecutorService executor = Executors.newFixedThreadPool(messageMetadata.getNumberOfConsumers());
		List<Future<String>> threads = new ArrayList<>();

		IntStream.rangeClosed(1, messageMetadata.getNumberOfConsumers()).forEach(i -> {
			threads.add(executor.submit(new RPCSocketServer(serverSocket)));
		});

		// Send the message to Kafka
		StringProducer producer = new StringProducer(producerProperties);
		producer.send(messageMetadata, requestMessage);
		producer.close();

		// Now listen and wait until timeout or message received
		threads.forEach(thread -> {
			try {
				responseList.add(thread.get(timeoutMillis, TimeUnit.MILLISECONDS));
			} catch (TimeoutException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});

		executor.shutdown();
		serverSocket.close();

		// Return the list of response as a JSON array
		return responseList.size() >= messageMetadata.getNumberOfConsumers() ? (responseList.size() == 1
				? Optional.of(responseList.get(0)) : Optional.of("[" + String.join(", ", responseList) + "]"))
				: Optional.empty();
	}
}
