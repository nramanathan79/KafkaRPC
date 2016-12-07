package com.easyapp.testintegration.kafka.sparkstream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.KafkaException;

import com.easyapp.integration.kafka.sparkstream.SparkStreamException;
import com.easyapp.integration.kafka.sparkstream.StringSparkStream;
import com.easyapp.integration.kafka.util.KafkaProperties;

public class TestSparkStream {

	public static void main(String[] args) {
		if (args.length > 0) {
			ExecutorService executor = Executors.newSingleThreadExecutor();

			try {
				final Properties consumerProperties = KafkaProperties.getKafkaSparkStreamProperties();
				consumerProperties.put("stream.message.processor.class",
						"com.easyapp.testintegration.kafka.sparkstream.TestStreamMessageProcessor");

				executor.submit(new StringSparkStream(consumerProperties, Arrays.asList(args))).get();
			} catch (InterruptedException | ExecutionException | KafkaException | SparkStreamException e) {
				e.printStackTrace();
			} finally {
				executor.shutdown();
			}
		} else {
			System.out.println("Usage: TestSparkStream <topic1> <topic2> ... <topicN>");
		}
	}
}
