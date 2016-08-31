package com.easyapp.integration.kafka.restcontroller;

import java.util.Optional;
import java.util.UUID;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.easyapp.integration.kafka.bean.MessageMetadata;
import com.easyapp.integration.kafka.producer.StringProducer;

@RestController
public class KafkaProducerRestController<T> {
	public static final String SEND_SYNC = "sync";
	public static final String SEND_ASYNC = "async";

	@Autowired
	private StringProducer producer;

	@RequestMapping(value = "/api/kafkaSend/{topic}", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> callRPCDirect(@PathVariable("topic") final String topic,
			@RequestParam(value = "key", required = false) final String key,
			@RequestParam(value = "sendMethod", required = false) final String sendMethod,
			@RequestBody @Valid String requestMessage) {
		MessageMetadata messageMetadata = MessageMetadata
				.getMessageMetadata(key == null ? UUID.randomUUID().toString() : key, topic);
		Optional<MessageMetadata> responseMessageMetadata = Optional.of(messageMetadata);

		if (SEND_ASYNC.equals(sendMethod)) {
			producer.sendAsync(messageMetadata, requestMessage);
		} else {
			responseMessageMetadata = producer.sendSync(messageMetadata, requestMessage);
		}

		return new ResponseEntity<>(responseMessageMetadata.get().toJSON(), HttpStatus.OK);
	}
}
