package com.easyapp.testintegration.kafka.rpc;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.easyapp.integration.kafka.consumer.StringConsumer;
import com.easyapp.integration.kafka.util.KafkaProperties;

public class TestConsumerRPC {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			final Properties consumerProperties = KafkaProperties.getKafkaConsumerProperties();
			consumerProperties.put("message.processor.class",
					"com.easyapp.testintegration.kafka.rpc.TestMessageProcessorRPC");

			ExecutorService executor = Executors.newSingleThreadExecutor();

			try {
				System.out.println("Total messages proceessed = "
						+ executor.submit(new StringConsumer(consumerProperties, args[0])).get());
			} finally {
				executor.shutdown();
			}
		} else {
			System.out.println("Usage: TestConsumerRPC <topic>");
		}
	}
}
