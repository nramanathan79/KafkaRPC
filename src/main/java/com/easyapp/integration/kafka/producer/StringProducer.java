package com.easyapp.integration.kafka.producer;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import com.easyapp.integration.kafka.bean.MessageMetadata;
import com.easyapp.integration.kafka.util.KafkaProperties;

@Component
public class StringProducer {
	private Producer<String, String> producer;

	public StringProducer() {
		this(KafkaProperties.getKafkaProducerProperties());
	}

	public StringProducer(final Properties producerProperties) {
		producer = new KafkaProducer<>(producerProperties);
	}

	public Optional<MessageMetadata> sendSync(final MessageMetadata messageMetadata, final String message) {
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

	public Future<RecordMetadata> sendAsync(final MessageMetadata messageMetadata, final String message) {
		return producer.send(
				new ProducerRecord<String, String>(messageMetadata.getTopic(), messageMetadata.toJSON(), message));
	}

	public Future<RecordMetadata> sendAsync(final MessageMetadata messageMetadata, final String message,
			final Callback callback) {
		return producer.send(
				new ProducerRecord<String, String>(messageMetadata.getTopic(), messageMetadata.toJSON(), message),
				callback);
	}

	public void close() {
		producer.close();
	}
}
