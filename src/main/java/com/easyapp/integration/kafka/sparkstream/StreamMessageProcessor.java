package com.easyapp.integration.kafka.sparkstream;

import java.io.Serializable;

import scala.Tuple2;

public interface StreamMessageProcessor<K, V> extends Serializable {
	void process(final Tuple2<K, V> record);
}
