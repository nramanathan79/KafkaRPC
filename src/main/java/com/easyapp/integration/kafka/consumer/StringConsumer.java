package com.easyapp.integration.kafka.consumer;

import java.util.Properties;

import com.easyapp.integration.kafka.util.KafkaProperties;

public class StringConsumer extends Consumer<String, String> {

	public StringConsumer() {
		super(KafkaProperties.getKafkaConsumerProperties(), 100L);
	}

	public StringConsumer(final long pollingInterfalMillis) {
		super(KafkaProperties.getKafkaConsumerProperties(), pollingInterfalMillis);
	}

	public StringConsumer(final Properties consumerProperties, final long pollingInterfalMillis) {
		super(consumerProperties, pollingInterfalMillis);
	}
}