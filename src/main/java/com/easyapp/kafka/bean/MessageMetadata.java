package com.easyapp.kafka.bean;

public class MessageMetadata extends BaseMetadata {
	private int partition;
	private long offset;

	protected MessageMetadata(final String key, final String topic) {
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
