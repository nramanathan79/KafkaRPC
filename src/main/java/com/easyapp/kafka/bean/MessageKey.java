package com.easyapp.kafka.bean;

public class MessageKey {
	private final String topic;
	private final String key;
	private int partition;
	private long offset;
	
	public MessageKey(final String topic, final String key) {
		this.topic = topic;
		this.key = key;
	}
	
	public MessageKey(final String topic, final String key, final int partition, final long offset) {
		this.topic = topic;
		this.key = key;
		this.partition = partition;
		this.offset = offset;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(final int partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(final long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public String getKey() {
		return key;
	}
}
