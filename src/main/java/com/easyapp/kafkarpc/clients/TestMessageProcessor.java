package com.easyapp.kafkarpc.clients;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class TestMessageProcessor extends ConsumerPartitionCallable<String, String> {

	public TestMessageProcessor(Properties consumerProperties, TopicPartition topicPartition,
			long pollingIntervalMillis) {
		super(consumerProperties, topicPartition, pollingIntervalMillis);
	}

	@Override
	void process(ConsumerRecord<String, String> record) {
		System.out.println(record.value());
	}
}
