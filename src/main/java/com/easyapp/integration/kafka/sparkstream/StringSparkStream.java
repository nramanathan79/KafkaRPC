package com.easyapp.integration.kafka.sparkstream;

import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.common.KafkaException;

public class StringSparkStream extends SparkStream<String, String> {
	private static final long serialVersionUID = 1L;

	public StringSparkStream(final Properties sparkStreamProperties, final Collection<String> topics) throws SparkStreamException {
		super(sparkStreamProperties, topics);
	}

	public StringSparkStream(final Collection<String> topics) throws KafkaException, SparkStreamException {
		super(topics);
	}
}
