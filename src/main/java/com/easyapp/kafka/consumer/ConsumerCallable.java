package com.easyapp.kafka.consumer;

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

public class ConsumerCallable<K, V> implements Callable<Long> {
	private final Properties consumerProperties;
	private final String topic;
	private final Class<? extends MessageProcessor<K, V>> consumerPartitionProcessorClass;
	private final long pollingIntervalMillis;

	public ConsumerCallable(final Properties consumerProperties,
			final Class<? extends MessageProcessor<K, V>> consumerPartitionProcessorClass,
			final long pollingIntervalMillis) {
		this.consumerProperties = consumerProperties;
		this.topic = consumerProperties.getProperty("topic");
		this.consumerPartitionProcessorClass = consumerPartitionProcessorClass;
		this.pollingIntervalMillis = pollingIntervalMillis;
	}

	@Override
	public Long call() throws Exception {
		// Return the future threads for consumers to wait on.
		List<Long> recordsProcessed = new ArrayList<>();

		KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		consumer.close();

		if (partitions != null && !partitions.isEmpty()) {
			ExecutorService executor = Executors.newFixedThreadPool(partitions.size());

			final Class<?>[] constructorParameterClasses = new Class[] { Properties.class, TopicPartition.class,
					long.class };

			List<Future<Long>> threads = new ArrayList<>();
			
			partitions.forEach(partition -> {
				try {
					threads.add(executor.submit(consumerPartitionProcessorClass
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
