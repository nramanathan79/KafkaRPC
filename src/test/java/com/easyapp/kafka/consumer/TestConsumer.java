package com.easyapp.kafka.consumer;

import java.util.Properties;

import com.easyapp.kafka.util.KafkaProperties;

public class TestConsumer {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			boolean daemonize = false;

			if (args.length > 1) {
				daemonize = ("-d").equals(args[1].trim());
			}

			final Properties consumerProperties = KafkaProperties.getKafkaConsumerProperties();
			StringConsumer consumer = new StringConsumer(consumerProperties, 100L, daemonize);
			System.out.println("Total messages proceessed = " + consumer.consume(args[0], TestMessageProcessor.class));
		}
		else {
			System.out.println("Usage: TestConsumerRPC <topic> [-d]");
		}
	}
}
