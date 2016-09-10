package com.easyapp.testintegration.kafka.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;
import com.easyapp.integration.kafka.rpc.RPCService;
import com.easyapp.integration.kafka.rpc.StringRPC;
import com.easyapp.integration.kafka.util.KafkaProperties;

public class TestRPC implements Callable<String> {
	private static final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();
	private static final RPCService rpcService = new RPCService();
	private static final StringRPC rpc = new StringRPC(rpcService, rpcProperties);

	private final String topic;
	private final String message;
	private final int index;

	public TestRPC(final String topic, final String fileName, final int index) {
		this.topic = topic;
		this.message = getMessage(fileName);
		this.index = index;
	}

	@Override
	public String call() throws Exception {
		String returnValue = "TIMED OUT";

		try {
			RPCMessageMetadata messageMetadata = RPCMessageMetadata.getDirectRPCMessageMetadata(String.valueOf(index),
					topic, InetAddress.getLocalHost(), Integer.parseInt(rpcProperties.getProperty("response.port")));

			Optional<String> response = rpc.rpcCall(messageMetadata,
					message.replaceFirst("<ID>", messageMetadata.getKey()), 10000);

			if (response.isPresent()) {
				returnValue = response.get();
			}

			System.out.println("Message: " + index + " sent to topic: " + topic + " returned response: " + returnValue);
		} catch (IOException | NumberFormatException e) {
			e.printStackTrace();
		}

		return returnValue;
	}

	public static final String getMessage(final String fileName) {
		try {
			return Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8).stream()
					.collect(Collectors.joining());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return "";
		}
	}

	public static void main(String[] args) {
		if (args.length > 1) {
			ExecutorService executor = Executors.newFixedThreadPool(50);
			List<Future<String>> threads = new ArrayList<>();

			long startTime = System.currentTimeMillis();

			IntStream.rangeClosed(1, 50).forEach(i -> {
				threads.add(executor.submit(new TestRPC(args[0], args[1], i)));
			});

			threads.forEach(thread -> {
				try {
					thread.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});
			
			long endTime = System.currentTimeMillis();

			System.out.println("Processed 50 RPC messages in " + (endTime - startTime) + "ms with average time = " + (endTime - startTime) / 50.0d + "ms.");

			rpcService.destroy();
			rpc.destroy();
			executor.shutdown();
		} else {
			System.out.println("Usage: TestRPC <topic> <input file URI>");
		}
	}
}
