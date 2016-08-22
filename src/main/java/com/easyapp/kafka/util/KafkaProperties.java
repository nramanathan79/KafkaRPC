package com.easyapp.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONException;
import org.json.JSONObject;

public class KafkaProperties {
	private static Properties getKafkaProperties(final String fileName) throws Exception {
		Properties properties = new Properties();
		InputStream input = null;

		try {
			// open the properties file
			input = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);

			// load a properties file
			properties.load(input);
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

		String bootstrapServers = properties.getProperty("bootstrap.servers");

		if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
			String zookeeperConnect = properties.getProperty("zookeeper.connect");

			if (zookeeperConnect == null || zookeeperConnect.trim().isEmpty()) {
				throw new Exception(
						"Either one of the properties: bootstrap.servers or zookeeper.connect must be present.");
			}

			ZooKeeper zk = new ZooKeeper(zookeeperConnect, 10000, null);
			List<String> brokerList = new ArrayList<String>();

			zk.getChildren("/brokers/ids", false).forEach(id -> {
				try {
					JSONObject json = new JSONObject(new String(zk.getData("/brokers/ids/" + id, false, null)));
					brokerList.add(json.getString("host") + ":" + json.getInt("port"));
				} catch (JSONException | KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			});

			properties.put("bootstrap.servers", String.join(",", brokerList));

			zk.close();
		}
		
		return properties;
	}
	
	public static Properties getKafkaConsumerProperties() throws Exception {
		Properties consumerProperties = getKafkaProperties("kafka.consumer.properties");

		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return consumerProperties;
	}

	public static Properties getKafkaProducerProperties() throws Exception {
		Properties producerProperties = getKafkaProperties("kafka.producer.properties");

		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return producerProperties;
	}
}
