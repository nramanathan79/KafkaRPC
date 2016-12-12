package com.easyapp.testintegration.kafka.sparkstream;

import com.easyapp.integration.kafka.sparkstream.StreamMessageProcessor;

import scala.Tuple2;

public class TestStreamMessageProcessor implements StreamMessageProcessor<String, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public void process(final Tuple2<String, String> record) {
		System.out.println("RAMBO: KEY = " + record._1() + ", VALUE = " + record._2());
	}
}
