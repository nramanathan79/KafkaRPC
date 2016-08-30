package com.easyapp.integration.kafka.rpc;

import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;
import com.easyapp.integration.kafka.consumer.MessageProcessor;
import com.easyapp.integration.kafka.producer.StringProducer;

public abstract class MessageProcessorRPC extends MessageProcessor<String, String> {
	private final StringProducer producer;

	public MessageProcessorRPC(final Properties consumerProperties, final TopicPartition topicPartition,
			final long pollingIntervalMillis) {
		super(consumerProperties, topicPartition, pollingIntervalMillis);

		Properties producerProperties = new Properties();
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
				consumerProperties.get(ConsumerConfig.CLIENT_ID_CONFIG));
		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		this.producer = new StringProducer(producerProperties);
	}

	@Override
	protected void consume(final ConsumerRecord<String, String> record) {
		Optional<RPCMessageMetadata> messageMetadata = RPCMessageMetadata.fromJSON(record.key(),
				RPCMessageMetadata.class);

		if (messageMetadata.isPresent()) {
			RPCMessageMetadata rpcMessageMetadata = messageMetadata.get();

			if (rpcMessageMetadata.getReplyTopic().isPresent()) {
				producer.sendAsync(rpcMessageMetadata.getUpdatedMessageMetadata(rpcMessageMetadata.getReplyTopic().get()),
						process(record));
			} else {
				RPCSocketClient.send(rpcMessageMetadata, process(record));
			}
		}
	}

	@Override
	protected void close() {
		producer.close();
		super.close();
	}
}
