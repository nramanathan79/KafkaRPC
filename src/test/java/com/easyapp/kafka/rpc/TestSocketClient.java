package com.easyapp.kafka.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import com.easyapp.kafka.bean.RPCMessageMetadata;

public class TestSocketClient {
	public static void main(String[] args) {
		try {
			RPCMessageMetadata messageMetadata = RPCMessageMetadata.getDirectRPCMessageMetadata("TEST", "test",
					InetAddress.getLocalHost(), 4000);
			Optional<String> acknowledgement = RPCSocketClient.send(messageMetadata, "Hello World");
			System.out.println(acknowledgement.isPresent() ? acknowledgement.get() : "NO RESPONSE");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
