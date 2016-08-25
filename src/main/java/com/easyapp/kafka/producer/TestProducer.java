package com.easyapp.kafka.producer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.easyapp.kafka.bean.MessageMetadata;
import com.easyapp.kafka.util.KafkaProperties;

public class TestProducer {
	public static final String getMessage(final String fileName) {
		try {
			return Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8).stream()
					.collect(Collectors.joining());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return "";
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length > 1) {
			final Properties producerProperties = KafkaProperties.getKafkaProducerProperties();
			final StringProducer producer = new StringProducer(producerProperties);
			
			final String topic = args[0];
			final String message = getMessage(args[1]);

			IntStream.rangeClosed(1, 100).forEach(i -> {
				final MessageMetadata messageMetadata = MessageMetadata.getMessageMetadata(UUID.randomUUID().toString(), topic);
				final MessageMetadata producedMessageMetadata = producer.send(messageMetadata, message.replaceFirst("(id)", messageMetadata.getKey()));

				System.out.println("Message + " + i + " sent to topic-partition: " + producedMessageMetadata.getTopic() + "-"
						+ producedMessageMetadata.getPartition() + " is stored at offset: "
						+ producedMessageMetadata.getOffset());
			});
			
			producer.close();
		} else {
			System.out.println("Usage: TestProducer <topic> <input file URI>");
		}
	}
}
