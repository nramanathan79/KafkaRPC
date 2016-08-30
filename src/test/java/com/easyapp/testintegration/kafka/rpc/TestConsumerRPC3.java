package com.easyapp.testintegration.kafka.rpc;

import java.util.Properties;

import com.easyapp.integration.kafka.consumer.Consumer;
import com.easyapp.integration.kafka.consumer.StringConsumer;
import com.easyapp.integration.kafka.util.KafkaProperties;

public class TestConsumerRPC3 {

	public static void main(String[] args) throws Exception {
		Properties consumerProperties = new Properties();

		consumerProperties.put("zookeeper.connect", "localhost:2181");
		consumerProperties.put("client.id", "consumer3");
		consumerProperties.put("session.timeout.ms", 30000);
		consumerProperties.put("enable.auto.commit", false);
		consumerProperties.put("auto.offset.reset", "earliest");

		if (args.length > 0) {
			System.out.println("Total messages proceessed = "
					+ new StringConsumer(KafkaProperties.getValidatedConsumerProperties(consumerProperties),
							Consumer.DEFAULT_POLLING_INTERVAL_MILLIS).consume(args[0], TestMessageProcessorRPC.class));
		} else {
			System.out.println("Usage: TestConsumerRPC3 <topic>");
		}
	}
}
