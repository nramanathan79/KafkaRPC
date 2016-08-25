package com.easyapp.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.easyapp.kafka.bean.MessageMetadata;

public class ProducerCallback implements Callback {
	private MessageMetadata messageMetadata;

	public ProducerCallback(final MessageMetadata messageMetadata) {
		this.messageMetadata = messageMetadata;
	}

	@Override
	public void onCompletion(RecordMetadata record, Exception exception) {
		if (exception != null) {
			exception.printStackTrace();
		}

		if (record != null) {
			messageMetadata = messageMetadata.getUpdatedMessageMetadata(record.partition(), record.offset());
		}
	}

	public MessageMetadata getMessageMetadata() {
		return messageMetadata;
	}
}
