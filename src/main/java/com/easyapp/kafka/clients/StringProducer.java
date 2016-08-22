package com.easyapp.kafka.clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.easyapp.kafka.bean.MessageKey;

public class StringProducer {
	private final Properties producerProperties;

	public StringProducer(final Properties producerProperties) {
		this.producerProperties = producerProperties;
	}

	public MessageKey produce(MessageKey messageKey, String message) {
		Producer<String, String> producer = new KafkaProducer<>(producerProperties);
		ProducerCallback callback = new ProducerCallback(messageKey);
		producer.send(new ProducerRecord<String, String>(messageKey.getTopic(), messageKey.getKey(), message),
				callback);
		producer.close();

		return callback.getMessageKey();
	}
}
