package com.easyapp.integration.kafka.sparkstream;

public class SparkStreamException extends Exception {
	private static final long serialVersionUID = 1L;

	public SparkStreamException() {
		super();
	}

	public SparkStreamException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public SparkStreamException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public SparkStreamException(final String message) {
		super(message);
	}

	public SparkStreamException(final Throwable cause) {
		super(cause);
	}
}
