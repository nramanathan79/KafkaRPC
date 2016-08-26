package com.easyapp.kafka.consumer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class StringConsumer {
	private final Properties consumerProperties;
	private final long pollingIntervalMillis;
	private final boolean daemonize;

	public StringConsumer(final Properties consumerProperties, final long pollingInterfalMillis,
			final boolean daemonize) {
		this.consumerProperties = consumerProperties;
		this.pollingIntervalMillis = pollingInterfalMillis;
		this.daemonize = daemonize;
	}

	public long consume(final String topic,
			final Class<? extends MessageProcessor<String, String>> messageProcessorClass) {
		ExecutorService executor = daemonize ? Executors.newSingleThreadExecutor(new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setDaemon(true);
				return t;
			}
		}) : Executors.newSingleThreadExecutor();

		Future<Long> kafkaConsumer = executor.submit(
				new ConsumerCallable<String, String>(consumerProperties, topic, messageProcessorClass, pollingIntervalMillis));

		long totalMessagesProcessed = 0;

		try {
			totalMessagesProcessed = kafkaConsumer.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}

		return totalMessagesProcessed;
	}
}
