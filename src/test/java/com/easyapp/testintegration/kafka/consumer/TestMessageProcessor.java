package com.easyapp.testintegration.kafka.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.easyapp.integration.kafka.consumer.MessageProcessor;

public class TestMessageProcessor extends MessageProcessor<String, String> {

	public TestMessageProcessor(final Properties consumerProperties, final TopicPartition topicPartition,
			long pollingIntervalMillis) {
		super(consumerProperties, topicPartition, pollingIntervalMillis);
	}

	@Override
	protected String process(final ConsumerRecord<String, String> record) {
		return record.value();
	}
}
