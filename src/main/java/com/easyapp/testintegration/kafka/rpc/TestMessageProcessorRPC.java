package com.easyapp.testintegration.kafka.rpc;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.easyapp.integration.kafka.rpc.MessageProcessorRPC;

public class TestMessageProcessorRPC extends MessageProcessorRPC {

	public TestMessageProcessorRPC(final Properties consumerProperties, final TopicPartition topicPartition,
			final long pollingIntervalMillis) {
		super(consumerProperties, topicPartition, pollingIntervalMillis);
	}

	@Override
	protected String process(final ConsumerRecord<String, String> record) {
		return record.value();
	}
}
