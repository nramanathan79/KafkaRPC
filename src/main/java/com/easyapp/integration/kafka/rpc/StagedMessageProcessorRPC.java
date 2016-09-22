package com.easyapp.integration.kafka.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;

public class StagedMessageProcessorRPC extends MessageProcessorRPC {
	private static final Map<String, List<String>> messageMap = new ConcurrentHashMap<>();
	private final int numberOfConsumers;

	public StagedMessageProcessorRPC(Properties consumerProperties, TopicPartition topicPartition,
			long pollingIntervalMillis) {
		super(consumerProperties, topicPartition, pollingIntervalMillis);
		numberOfConsumers = Integer.parseInt(String.valueOf(consumerProperties.get("number.of.consumers")));
	}

	@Override
	protected String process(ConsumerRecord<String, String> record) {
		return record.value();
	}

	@Override
	protected void consume(final ConsumerRecord<String, String> record) {
		Optional<RPCMessageMetadata> messageMetadata = RPCMessageMetadata.fromJSON(record.key(),
				RPCMessageMetadata.class);

		if (messageMetadata.isPresent()) {
			RPCMessageMetadata rpcMessageMetadata = messageMetadata.get();

			List<String> result = messageMap.get(rpcMessageMetadata.getKey());

			if (result == null) {
				result = new ArrayList<>();
			}

			result.add(process(record));
			messageMap.put(rpcMessageMetadata.getKey(), result);

			if (result.size() >= numberOfConsumers) {
				RPCSocketClient.send(rpcMessageMetadata,
						numberOfConsumers > 1 ? "[" + String.join(", ", result) + "]" : result.get(0));

				messageMap.remove(rpcMessageMetadata.getKey());
			}
		}
	}
}
