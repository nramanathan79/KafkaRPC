package com.easyapp.integration.kafka.consumer;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.easyapp.integration.kafka.util.KafkaProperties;

public class Consumer<K, V> implements Callable<Long> {
	private final Properties consumerProperties;
	private final String topic;
	private final long pollingIntervalMillis;
	private final Class<? extends MessageProcessor<String, String>> messageProcessorClass; 

	public Consumer(final String topic) throws ClassNotFoundException {
		this(KafkaProperties.getKafkaConsumerProperties(), topic);
	}
	
	@SuppressWarnings("unchecked")
	public Consumer(final Properties consumerProperties, final String topic) throws ClassNotFoundException {
		this.consumerProperties = consumerProperties;
		this.topic = topic;
		this.pollingIntervalMillis = Long.parseLong(consumerProperties.getProperty("polling.interval.ms"));
		this.messageProcessorClass = (Class<? extends MessageProcessor<String, String>>) Class.forName(consumerProperties.getProperty("message.processor.class"));
	}

	public Properties getConsumerProperties(final int partition) {
		Properties partitionConsumerProperties = new Properties();

		consumerProperties.keySet().forEach(key -> {
			Object value = consumerProperties.get(key);

			if (("client.id").equals(key)) {
				value += String.valueOf(partition);
			}

			partitionConsumerProperties.put(key, value);
		});

		return partitionConsumerProperties;
	}

	@Override
	public Long call() throws Exception {
		// Return the future threads for consumers to wait on.
		List<Long> recordsProcessed = new ArrayList<>();

		final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
		final List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		consumer.close();

		if (partitions != null && !partitions.isEmpty()) {
			ExecutorService executor = Executors.newFixedThreadPool(partitions.size());

			final Class<?>[] constructorParameterClasses = new Class[] { Properties.class, TopicPartition.class,
					long.class };

			List<Future<Long>> threads = new ArrayList<>();

			partitions.forEach(partition -> {
				try {
					threads.add(executor.submit(messageProcessorClass
							.getDeclaredConstructor(constructorParameterClasses).newInstance(getConsumerProperties(partition.partition()),
									new TopicPartition(topic, partition.partition()), pollingIntervalMillis)));
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
						| InvocationTargetException | NoSuchMethodException | SecurityException e) {
					e.printStackTrace();
				}
			});

			threads.forEach(thread -> {
				try {
					recordsProcessed.add(thread.get());
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});

			executor.shutdown();
		}

		return recordsProcessed.stream().mapToLong(Long::longValue).sum();
	}
}
