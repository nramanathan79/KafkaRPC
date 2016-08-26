package com.easyapp.kafka.rpc;

import com.easyapp.kafka.consumer.StringConsumer;
import com.easyapp.kafka.util.KafkaProperties;

public class TestConsumerRPC {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			boolean daemonize = false;

			if (args.length > 1) {
				daemonize = ("-d").equals(args[1].trim());
			}

			StringConsumer consumer = new StringConsumer(KafkaProperties.getKafkaConsumerProperties(), 100L, daemonize);
			System.out
					.println("Total messages proceessed = " + consumer.consume(args[0], TestMessageProcessorRPC.class));
		} else {
			System.out.println("Usage: TestConsumerRPC <topic> [-d]");
		}
	}
}
