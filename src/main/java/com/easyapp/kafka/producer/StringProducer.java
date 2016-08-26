package com.easyapp.kafka.producer;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.easyapp.kafka.bean.MessageMetadata;

public class StringProducer {
	private final Producer<String, String> producer;

	public StringProducer(final Properties producerProperties) {
		producer = new KafkaProducer<>(producerProperties);
	}

	public Optional<MessageMetadata> send(final MessageMetadata messageMetadata, final String message) {
		try {
			RecordMetadata record = producer.send(
					new ProducerRecord<String, String>(messageMetadata.getTopic(), messageMetadata.toJSON(), message))
					.get();
			return Optional.of(messageMetadata.getUpdatedMessageMetadata(record.partition(), record.offset()));
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Optional.empty();
		}
	}

	public void close() {
		producer.close();
	}
}
