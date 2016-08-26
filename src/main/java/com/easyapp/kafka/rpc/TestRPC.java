package com.easyapp.kafka.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.easyapp.kafka.bean.RPCMessageMetadata;
import com.easyapp.kafka.util.KafkaProperties;

public class TestRPC {
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
			final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();
			final StringRPC rpc = new StringRPC(rpcProperties, 10000);

			final String topic = args[0];
			final String message = getMessage(args[1]);

			IntStream.rangeClosed(1, 100).forEach(i -> {
				RPCMessageMetadata messageMetadata;
				try {
					messageMetadata = RPCMessageMetadata.getDirectRPCMessageMetadata(UUID.randomUUID().toString(),
							topic, InetAddress.getLocalHost(),
							Integer.parseInt(rpcProperties.getProperty("response.port")));

					Optional<String> response = rpc.rpcCall(messageMetadata,
							message.replaceFirst("(id)", messageMetadata.getKey()));

					System.out.println("Message: " + i + " sent to topic: " + topic + " returned response: "
							+ (response.isPresent() ? response.get() : "TIMED OUT"));
				} catch (IOException | NumberFormatException e) {
					e.printStackTrace();
				}
			});
		} else {
			System.out.println("Usage: TestRPC <topic> <input file URI>");
		}
	}
}
