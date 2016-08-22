package com.easyapp.kafkarpc.clients;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestConsumer {
	private static Properties getKafkaConsumerProperties(String propertiesFileName) {
		Properties consumerProperties = new Properties();
		InputStream input = null;

		try {
			// open the properties file
			input = new FileInputStream(propertiesFileName);

			// load a properties file
			consumerProperties.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return consumerProperties;
	}

	public static void main(String[] args) {
		boolean daemonize = false;

		if (args.length > 0) {
			daemonize = ("-d").equals(args[0].trim());
		}

		Properties consumerProperties = getKafkaConsumerProperties("kafka.consumer.properties");
		StringConsumer consumer = new StringConsumer(consumerProperties, 100L, daemonize);
		System.out.println("Total messages proceessed = "
				+ consumer.consume(consumerProperties.getProperty("topic"), TestMessageProcessor.class));
	}
}
