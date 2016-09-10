package com.easyapp.integration.kafka.consumer;

import java.util.Properties;

public class StringConsumer extends Consumer<String, String> {

	public StringConsumer(final String topic) throws ClassNotFoundException {
		super(topic);
	}

	public StringConsumer(final Properties consumerProperties, final String topic) throws ClassNotFoundException {
		super(consumerProperties, topic);
	}
}
