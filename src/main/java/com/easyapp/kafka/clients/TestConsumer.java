package com.easyapp.kafka.clients;

import java.util.Properties;

import com.easyapp.kafka.util.KafkaProperties;

public class TestConsumer {

	public static void main(String[] args) throws Exception {
		boolean daemonize = false;

		if (args.length > 0) {
			daemonize = ("-d").equals(args[0].trim());
		}

		final Properties consumerProperties = KafkaProperties.getKafkaConsumerProperties();
		StringConsumer consumer = new StringConsumer(consumerProperties, 100L, daemonize);
		System.out.println("Total messages proceessed = "
				+ consumer.consume(consumerProperties.getProperty("topic"), TestMessageProcessor.class));
	}
}
