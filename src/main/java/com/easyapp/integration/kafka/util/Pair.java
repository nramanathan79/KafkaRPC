package com.easyapp.integration.kafka.util;

import java.io.Serializable;

public class Pair<K, V> implements Serializable {
	private static final long serialVersionUID = 1L;

	private K key;
	private V value;
	
	public Pair() {
		
	}
	
	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(final K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(final V value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public String toString() {
		return key.toString() + ":" + value.toString();
	}
}
