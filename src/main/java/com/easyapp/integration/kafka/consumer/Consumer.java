package com.easyapp.integration.kafka.consumer;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class Consumer<K, V> {
	private final Properties consumerProperties;
	private final long pollingIntervalMillis;

	public Consumer(final Properties consumerProperties, final long pollingIntervalMillis) {
		this.consumerProperties = consumerProperties;
		this.pollingIntervalMillis = pollingIntervalMillis;
	}

	public long consume(final String topic,
			final Class<? extends MessageProcessor<String, String>> messageProcessorClass) {
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
							.getDeclaredConstructor(constructorParameterClasses).newInstance(consumerProperties,
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
