package com.easyapp.kafkarpc.clients;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public abstract class ConsumerPartitionCallable<K, V> implements Callable<Long> {
	private final KafkaConsumer<K, V> consumer;
	private final TopicPartition topicPartition;
	private final long pollingIntervalMillis;
	
	public ConsumerPartitionCallable(final Properties consumerProperties, final TopicPartition topicPartition, final long pollingIntervalMillis) {
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.topicPartition = topicPartition;
		this.pollingIntervalMillis = pollingIntervalMillis;
	}

	abstract void process(final ConsumerRecord<K, V> record);
	
	protected void commit() {
		consumer.commitAsync();
	}
	
	protected void close() {
		consumer.close();
	}
	
	@Override
	public Long call() throws Exception {
		long recordsProcessed = 0;
		
		try {
			consumer.assign(Arrays.asList(topicPartition));
			
			while (!Thread.currentThread().isInterrupted()) {
				// Listen on the stream an get records.
				ConsumerRecords<K, V> records = consumer.poll(pollingIntervalMillis);
				
				// process each record.
				records.forEach(record -> process(record));
				
				// Commit each batch. Default is Async. Override to change commit option.
				commit();
				
				// Keep count of the records processed.
				recordsProcessed += records.count();
			}
		}
		finally {
			// Close the consumer.
			close();
		}
		
		return recordsProcessed;
	}
}
