package com.easyapp.kafkarpc.clients;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.easyapp.kafkarpc.bean.MessageKey;

public class ProducerCallback implements Callback {
	private MessageKey messageKey;

	public ProducerCallback(final MessageKey messagekey) {
		this.messageKey = messagekey;
	}

	@Override
	public void onCompletion(RecordMetadata record, Exception exception) {
		if (exception != null) {
			exception.printStackTrace();
		}

		if (record != null) {
			messageKey = new MessageKey(messageKey.getTopic(), messageKey.getKey(), record.partition(),
					record.offset());
		}
	}
	
	public MessageKey getMessageKey() {
		return messageKey;
	}
}
