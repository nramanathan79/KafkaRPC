package com.easyapp.integration.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONException;
import org.json.JSONObject;

public class KafkaProperties {
	private static String resolveValueWithEnvVars(final String value) {
		if (value == null) {
			return value;
		}

		final Pattern envVarPattern = Pattern.compile("\\$\\{([A-Za-z0-9\\.]+)\\}");
		final Matcher envVarMatcher = envVarPattern.matcher(value);
		final StringBuffer buffer = new StringBuffer();

		while (envVarMatcher.find()) {
			String envVarValue = System.getenv(envVarMatcher.group(1));
			envVarMatcher.appendReplacement(buffer, envVarValue == null ? "" : Matcher.quoteReplacement(envVarValue));
		}

		envVarMatcher.appendTail(buffer);

		return buffer.toString();
	}

	private static Properties getKafkaProperties(final String fileName) throws KafkaException {
		Properties properties = new Properties();
		InputStream input = null;

		try {
			// open the properties file
			input = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);

			// load a properties file
			properties.load(input);
		} catch (IOException e) {
			throw new KafkaException(e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		Properties expandedEnvProperties = new Properties();

		properties.forEach((key, value) -> {
			expandedEnvProperties.put(key, resolveValueWithEnvVars(String.valueOf(value)));
		});

		return expandedEnvProperties;
	}

	private static Properties validateProperties(final Properties properties) throws KafkaException {
		Properties validatedProperties = new Properties();
		validatedProperties.putAll(properties);

		try {
			final String bootstrapServers = validatedProperties.getProperty("bootstrap.servers");

			if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
				final String zookeeperConnect = validatedProperties.getProperty("zookeeper.connect");

				if (zookeeperConnect == null || zookeeperConnect.trim().isEmpty()) {
					throw new KafkaException(
							"Either one of the properties: bootstrap.servers or zookeeper.connect must be present.");
				}

				ZooKeeper zk = new ZooKeeper(zookeeperConnect, 10000, new Watcher() {

					@Override
					public void process(WatchedEvent event) {
						// Nothing to do
					}
				});

				List<String> brokerList = new ArrayList<String>();

				zk.getChildren("/brokers/ids", false).forEach(id -> {
					try {
						JSONObject json = new JSONObject(new String(zk.getData("/brokers/ids/" + id, false, null)));
						brokerList.add(json.getString("host") + ":" + json.getInt("port"));
					} catch (JSONException | KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				});

				validatedProperties.put("bootstrap.servers", String.join(",", brokerList));
				validatedProperties.remove("zookeeper.conect");

				zk.close();
			}

			final String clientId = validatedProperties.getProperty("client.id");

			if (clientId == null || clientId.trim().isEmpty()) {
				throw new KafkaException("client.id property must be present");
			}
		} catch (IOException e) {
			throw new KafkaException(e);
		} catch (KeeperException e) {
			throw new KafkaException(e);
		} catch (InterruptedException e) {
			throw new KafkaException(e);
		}

		return validatedProperties;
	}

	private static Properties validateConsumerProperties(Properties properties) throws KafkaException {
		Properties validatedProperties = validateProperties(properties);

		final String groupId = validatedProperties.getProperty("group.id");

		if (groupId == null || groupId.trim().isEmpty()) {
			validatedProperties.put("group.id", validatedProperties.get("client.id"));
		}

		return validatedProperties;
	}

	public static Properties getValidatedConsumerProperties(Properties properties) throws KafkaException {
		Properties consumerProperties = validateConsumerProperties(properties);

		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return consumerProperties;
	}

	public static Properties getValidatedProducerProperties(Properties properties) throws KafkaException {
		Properties producerProperties = validateProperties(properties);

		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return producerProperties;
	}

	public static Properties getKafkaConsumerProperties() throws KafkaException {
		return getValidatedConsumerProperties(getKafkaProperties("kafka.consumer.properties"));
	}

	public static Properties getKafkaProducerProperties() throws KafkaException {
		return getValidatedProducerProperties(getKafkaProperties("kafka.producer.properties"));
	}

	public static Properties getKafkaRPCProperties() throws KafkaException {
		return getValidatedProducerProperties(getKafkaProperties("kafka.rpc.properties"));
	}
}
