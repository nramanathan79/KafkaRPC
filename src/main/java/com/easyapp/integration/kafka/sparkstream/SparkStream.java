package com.easyapp.integration.kafka.sparkstream;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.easyapp.integration.kafka.util.KafkaProperties;

import scala.Tuple2;

public class SparkStream<K, V> implements Callable<Void>, Serializable {
	private static final long serialVersionUID = 1L;

	private final Collection<String> topics;
	private final Map<String, Object> consumerParams;
	private final StreamMessageProcessor<K, V> streamMessageProcessor;
	private final long pollingIntervalMillis;
	private final String sparkMaster;
	private final String sparkAppName;
	private final String[] sparkDriverJars;
	private final Set<Tuple2<String, String>> sparkDriverJavaOptions = new HashSet<>();

	public SparkStream(final Collection<String> topics) throws KafkaException, SparkStreamException {
		this(KafkaProperties.getKafkaSparkStreamProperties(), topics);
	}

	@SuppressWarnings("unchecked")
	public SparkStream(final Properties sparkStreamProperties, final Collection<String> topics)
			throws SparkStreamException {
		this.topics = topics;
		this.consumerParams = new HashMap<>();

		sparkStreamProperties.forEach((key, value) -> consumerParams.put((String) key, value));
		try {
			final String streamMessageProcessorClassName = sparkStreamProperties
					.getProperty("stream.message.processor.class");
			final Class<?> streamMessageProcessorClass = Class.forName(streamMessageProcessorClassName);

			if (StreamMessageProcessor.class.isAssignableFrom(streamMessageProcessorClass)) {
				this.streamMessageProcessor = (StreamMessageProcessor<K, V>) streamMessageProcessorClass.newInstance();
			} else {
				this.streamMessageProcessor = null;
				throw new SparkStreamException(
						streamMessageProcessorClassName + " does not implement StreamMessageProcessor interface.");
			}
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new SparkStreamException(e);
		}

		this.pollingIntervalMillis = Long.parseLong(sparkStreamProperties.getProperty("polling.interval.ms"));
		this.sparkMaster = sparkStreamProperties.getProperty("spark.master");
		this.sparkAppName = sparkStreamProperties.getProperty("spark.app.name");

		final String sparkJars = sparkStreamProperties.getProperty("spark.driver.jars");
		if (sparkJars != null && !sparkJars.contains("spark.driver.jars")) {
			this.sparkDriverJars = sparkJars.trim().split(",");
		} else {
			this.sparkDriverJars = null;
		}

		final String sparkJavaOptions = sparkStreamProperties.getProperty("spark.driver.java.options");
		if (sparkJavaOptions != null && !sparkJavaOptions.contains("spark.driver.java.options")) {
			final String[] javaOptions = sparkJavaOptions.trim().split(" ");
			for (String javaOption : javaOptions) {
				if (javaOption.startsWith("-D")) {
					javaOption = javaOption.substring(2);
				}

				final String[] option = javaOption.split("=");
				this.sparkDriverJavaOptions.add(new Tuple2<>(option[0], option[1]));
			}
		}
	}

	@Override
	public Void call() throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);

		if (sparkDriverJars != null && sparkDriverJars.length > 0) {
			sparkConf = sparkConf.setJars(sparkDriverJars);
		}

		if (sparkDriverJavaOptions.size() > 0) {
			@SuppressWarnings("unchecked")
			Tuple2<String, String>[] options = new Tuple2[sparkDriverJavaOptions.size()];
			sparkConf.setExecutorEnv(sparkDriverJavaOptions.toArray(options));
		}

		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
				Durations.milliseconds(pollingIntervalMillis));

		final JavaInputDStream<ConsumerRecord<K, V>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, consumerParams));

		stream.mapToPair(new PairFunction<ConsumerRecord<K, V>, K, V>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<K, V> call(ConsumerRecord<K, V> record) throws Exception {
				return new Tuple2<>(record.key(), record.value());
			}
		}).foreachRDD(rdd -> rdd.collect().stream().forEach(record -> streamMessageProcessor.process(record)));

		streamingContext.start();

		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return null;
	}
}
