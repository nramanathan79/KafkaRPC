package com.easyapp.testintegration.kafka.consumer;

import com.easyapp.integration.kafka.consumer.StringConsumer;

public class TestConsumer {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			System.out.println(
					"Total messages proceessed = " + new StringConsumer().consume(args[0], TestMessageProcessor.class));
		} else {
			System.out.println("Usage: TestConsumerRPC <topic>");
		}
	}
}
