package com.easyapp.integration.kafka.bean;

import java.net.InetAddress;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RPCMessageMetadata extends MessageMetadata {
	
	@JsonProperty("replyHost")
	private final InetAddress replyHost;

	@JsonProperty("replyPort")
	private final int replyPort;

	@JsonProperty("replyTopic")
	private final Optional<String> replyTopic;
	
	@JsonProperty("numberOfConsumers")
	private final int numberOfConsumers;

	protected RPCMessageMetadata(@JsonProperty("key") final String key, @JsonProperty("topic") final String topic,
			@JsonProperty("replyHost") final InetAddress replyHost, @JsonProperty("replyPort") final int replyPort,
			@JsonProperty("replyTopicSuffix") final String replyTopicSuffix,
			@JsonProperty("numberOfConsumers") final int numberOfConsumers) {
		super(key, topic);
		this.replyHost = replyHost;
		this.replyPort = replyPort;
		this.replyTopic = replyTopicSuffix == null ? Optional.empty() : Optional.of(topic + "-" + replyTopicSuffix);
		this.numberOfConsumers = numberOfConsumers;
	}

	protected RPCMessageMetadata(final RPCMessageMetadata messageMetadata, final int partition, final long offset) {
		super(messageMetadata, partition, offset);
		this.replyTopic = messageMetadata.getReplyTopic();
		this.replyHost = messageMetadata.getReplyHost();
		this.replyPort = messageMetadata.getReplyPort();
		this.numberOfConsumers = messageMetadata.getNumberOfConsumers();
	}

	protected RPCMessageMetadata(final RPCMessageMetadata messageMetadata, final String topic) {
		super(messageMetadata, topic);
		this.replyTopic = messageMetadata.getReplyTopic();
		this.replyHost = messageMetadata.getReplyHost();
		this.replyPort = messageMetadata.getReplyPort();
		this.numberOfConsumers = messageMetadata.getNumberOfConsumers();
	}

	public static RPCMessageMetadata getDirectRPCMessageMetadata(final String key, final String topic,
			final InetAddress replyHost, final int replyPort) {
		return new RPCMessageMetadata(key, topic, replyHost, replyPort, null, 1);
	}

	public static RPCMessageMetadata getScatterGatherRPCMessageMetadata(final String key, final String topic,
			final InetAddress replyHost, final int replyPort, final int numberOfConsumers) {
		return new RPCMessageMetadata(key, topic, replyHost, replyPort, null, numberOfConsumers);
	}

	public static RPCMessageMetadata getStagedRPCMessageMetadata(final String key, final String topic,
			final InetAddress replyHost, final int replyPort) {
		return new RPCMessageMetadata(key, topic, replyHost, replyPort, "stage", 1);
	}

	@Override
	public MessageMetadata getUpdatedMessageMetadata(final int partition, final long offset) {
		return new RPCMessageMetadata(this, partition, offset);
	}

	@Override
	public MessageMetadata getUpdatedMessageMetadata(final String topic) {
		return new RPCMessageMetadata(this, topic);
	}

	public int getNumberOfConsumers() {
		return numberOfConsumers;
	}

	public Optional<String> getReplyTopic() {
		return replyTopic;
	}

	public InetAddress getReplyHost() {
		return replyHost;
	}

	public int getReplyPort() {
		return replyPort;
	}
}
