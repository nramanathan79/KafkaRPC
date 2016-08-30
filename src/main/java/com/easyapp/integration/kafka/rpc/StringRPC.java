package com.easyapp.integration.kafka.rpc;

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

import javax.annotation.PreDestroy;

import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;
import com.easyapp.integration.kafka.producer.StringProducer;
import com.easyapp.integration.kafka.util.KafkaProperties;

@Component
public class StringRPC {
	private RPCService rpcService;
	private final StringProducer producer;

	@Autowired
	public StringRPC(final RPCService rpcService) {
		this.rpcService = rpcService;
		this.producer = new StringProducer(KafkaProperties.getKafkaRPCProperties());
	}

	public StringRPC(final RPCService rpcService, final Properties producerProperties) {
		this.rpcService = rpcService;
		this.producer = new StringProducer(producerProperties);
	}

	public Optional<String> rpcCall(final RPCMessageMetadata messageMetadata, final String requestMessage,
			final long timeoutMillis) {
		List<String> responseList = new ArrayList<>();

		// Create threads to accept connections from responding RPC
		// consumers
		ExecutorService executor = Executors.newFixedThreadPool(messageMetadata.getNumberOfConsumers());
		List<Future<String>> threads = new ArrayList<>();

		rpcService.addToRegistry(messageMetadata.getKey());

		IntStream.rangeClosed(1, messageMetadata.getNumberOfConsumers()).forEach(i -> {
			threads.add(executor.submit(new MessageReceiver(messageMetadata.getKey(), rpcService, timeoutMillis)));
		});

		// Send the message to Kafka
		producer.sendAsync(messageMetadata, requestMessage);

		// Now listen and wait until timeout or message received
		threads.forEach(thread -> {
			try {
				responseList.add(thread.get(timeoutMillis, TimeUnit.MILLISECONDS));
			} catch (TimeoutException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
				throw new KafkaException(e);
			}
		});

		executor.shutdown();
		rpcService.removeFromRegistry(messageMetadata.getKey());

		// Return the list of response as a JSON array
		return responseList.size() >= messageMetadata.getNumberOfConsumers() ? (responseList.size() == 1
				? Optional.of(responseList.get(0)) : Optional.of("[" + String.join(", ", responseList) + "]"))
				: Optional.empty();
	}

	@PreDestroy
	public void destroy() {
		producer.close();
	}
}
