package com.easyapp.integration.kafka.restcontroller;

import java.io.IOException;
import java.net.InetAddress;
import java.util.NoSuchElementException;
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

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;
import com.easyapp.integration.kafka.rpc.RPCService;
import com.easyapp.integration.kafka.rpc.StringRPC;

@RestController
public class KafkaRPCRestController<T> {
	@Autowired
	private RPCService rpcService;

	@Autowired
	private StringRPC rpc;

	@RequestMapping(value = "/api/kafkaRPCDirect/{topic}", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> callRPCDirect(@PathVariable("topic") final String topic,
			@RequestParam(value = "timeoutMillis", required = false) final Long timeoutMillis,
			@RequestBody @Valid String requestMessage) {
		try {
			Optional<String> responseMessage = rpc.rpcCall(
					RPCMessageMetadata.getDirectRPCMessageMetadata(UUID.randomUUID().toString(), topic,
							InetAddress.getLocalHost(), rpcService.getRPCResponsePort()),
					requestMessage, timeoutMillis == null ? 10000 : timeoutMillis);

			return new ResponseEntity<>(responseMessage.get(), HttpStatus.OK);
		} catch (IOException | NumberFormatException | NoSuchElementException e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@RequestMapping(value = "/api/kafkaRPCScatterGather/{topic}/{numberOfConsumers}", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> callRPCScatterGather(@PathVariable("topic") final String topic,
			@PathVariable("numberOfConsumers") final int numberOfConsumers,
			@RequestParam(value = "timeoutMillis", required = false) final Long timeoutMillis,
			@RequestBody @Valid String requestMessage) {
		try {
			Optional<String> responseMessage = rpc.rpcCall(
					RPCMessageMetadata.getScatterGatherRPCMessageMetadata(UUID.randomUUID().toString(), topic,
							InetAddress.getLocalHost(), rpcService.getRPCResponsePort(), numberOfConsumers),
					requestMessage, timeoutMillis == null ? 10000 : timeoutMillis);

			return new ResponseEntity<>(responseMessage.get(), HttpStatus.OK);
		} catch (IOException | NumberFormatException | NoSuchElementException e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@RequestMapping(value = "/api/kafkaRPCStaged/{topic}", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> callRPCStaged(@PathVariable("topic") final String topic,
			@RequestParam(value = "timeoutMillis", required = false) final Long timeoutMillis,
			@RequestBody @Valid String requestMessage) {
		try {
			Optional<String> responseMessage = rpc.rpcCall(
					RPCMessageMetadata.getStagedRPCMessageMetadata(UUID.randomUUID().toString(), topic,
							InetAddress.getLocalHost(), rpcService.getRPCResponsePort()),
					requestMessage, timeoutMillis == null ? 10000 : timeoutMillis);

			return new ResponseEntity<>(responseMessage.get(), HttpStatus.OK);
		} catch (IOException | NumberFormatException | NoSuchElementException e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
}
