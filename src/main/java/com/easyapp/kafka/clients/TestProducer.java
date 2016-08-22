package com.easyapp.kafka.clients;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import com.easyapp.kafka.bean.MessageKey;
import com.easyapp.kafka.util.KafkaProperties;

public class TestProducer {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			final Properties producerProperties = KafkaProperties.getKafkaProducerProperties();
			StringProducer producer = new StringProducer(producerProperties);
			MessageKey messageKey = new MessageKey(producerProperties.getProperty("topic"),
					UUID.randomUUID().toString());

			String message = "";

			try {
				message = Files.readAllLines(Paths.get(args[0]), StandardCharsets.UTF_8).stream()
						.collect(Collectors.joining());
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			messageKey = producer.produce(messageKey, message.replaceFirst("(id)", messageKey.getKey()));

			System.out.println("Message sent to topic-partition: " + messageKey.getTopic() + "-"
					+ messageKey.getPartition() + " is stored at offset: " + messageKey.getOffset());
		} else {
			System.out.println("Command line argument message file URI is missing.");
		}
	}
}
