package com.easyapp.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.easyapp.kafka.bean.MessageMetadata;

public class StringProducer {
	private final Producer<String, String> producer;

	public StringProducer(final Properties producerProperties) {
		producer = new KafkaProducer<>(producerProperties);
	}

	public MessageMetadata send(final MessageMetadata messageMetadata, final String message) {
		ProducerCallback callback = new ProducerCallback(messageMetadata);
		producer.send(new ProducerRecord<String, String>(messageMetadata.getTopic(), messageMetadata.toJSON(), message),
				callback);

		return callback.getMessageMetadata();
	}

	public void close() {
		producer.close();
	}
}
