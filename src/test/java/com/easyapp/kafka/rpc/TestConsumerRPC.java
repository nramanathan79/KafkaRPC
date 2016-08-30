package com.easyapp.kafka.rpc;

import com.easyapp.kafka.consumer.StringConsumer;

public class TestConsumerRPC {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			System.out.println("Total messages proceessed = "
					+ new StringConsumer().consume(args[0], TestMessageProcessorRPC.class));
		} else {
			System.out.println("Usage: TestConsumerRPC <topic>");
		}
	}
}
