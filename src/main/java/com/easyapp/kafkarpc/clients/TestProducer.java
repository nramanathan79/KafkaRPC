package com.easyapp.kafkarpc.clients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.easyapp.kafkarpc.bean.MessageKey;

public class TestProducer {
	private static Properties getKafkaProducerProperties() {
		Properties producerProperties = new Properties();
		InputStream input = null;

		try {
			// open the properties file
			input = TestProducer.class.getResourceAsStream("kafka.producer.properties");
System.out.println(input);
			// load a properties file
			producerProperties.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return producerProperties;
	}

	public static void main(String[] args) {
		if (args.length > 0) {
			Properties producerProperties = getKafkaProducerProperties();
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
