package com.easyapp.kafka.bean;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class BaseMetadata {
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	private final String key;
	private final String topic;

	protected BaseMetadata(final String key, final String topic) {
		this.key = key;
		this.topic = topic;
	}

	public String getKey() {
		return key;
	}

	public String getTopic() {
		return topic;
	}

	public String toJSON() {
		try {
			return jsonMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return getKey();
		}
	}

	public static <T extends BaseMetadata> Optional<T> fromJSON(String jsonString, Class<T> keyClass) {
		try {
			return Optional.of(jsonMapper.readValue(jsonString, keyClass));
		} catch (IOException e) {
			e.printStackTrace();
			return Optional.empty();
		}
	}
}
