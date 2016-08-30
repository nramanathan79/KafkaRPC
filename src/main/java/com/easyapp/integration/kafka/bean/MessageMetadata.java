package com.easyapp.integration.kafka.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageMetadata extends BaseMetadata {
	
	@JsonProperty("partition")
	private int partition;
	
	@JsonProperty("offset")
	private long offset;

	protected MessageMetadata(@JsonProperty("key") final String key, @JsonProperty("topic") final String topic) {
		super(key, topic);
	}

	protected MessageMetadata(final MessageMetadata messageMetadata, final int partition, final long offset) {
		super(messageMetadata.getKey(), messageMetadata.getTopic());
		this.partition = partition;
		this.offset = offset;
	}

	protected MessageMetadata(final MessageMetadata messageMetadata, final String topic) {
		super(messageMetadata.getKey(), topic);
		this.partition = messageMetadata.getPartition();
		this.offset = messageMetadata.getOffset();
	}

	public static MessageMetadata getMessageMetadata(final String key, final String topic) {
		return new MessageMetadata(key, topic);
	}

	public MessageMetadata getUpdatedMessageMetadata(final int partition, final long offset) {
		return new MessageMetadata(this, partition, offset);
	}

	public MessageMetadata getUpdatedMessageMetadata(final String topic) {
		return new MessageMetadata(this, partition, offset);
	}

	public int getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}
}
