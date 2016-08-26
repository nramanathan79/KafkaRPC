package com.easyapp.kafka.rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.easyapp.kafka.util.KafkaProperties;

@Service
public class RPCService {
	private static final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();
	private static int rpcResponsePort;
	private static ServerSocket serverSocket = null;

	@PostConstruct
	public void init() {
		try {
			rpcResponsePort = Integer.parseInt(rpcProperties.getProperty("response.port"));
		} catch (Exception e) {
			rpcResponsePort = 11111;
		}

		try {
			serverSocket = new ServerSocket(rpcResponsePort);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static ServerSocket getRPCServerSocket() {
		return serverSocket;
	}

	public static Properties getRPCProperties() {
		return rpcProperties;
	}

	public static int getRPCResponsePort() {
		return rpcResponsePort;
	}

	public static void setRpcResponsePort(int rpcResponsePort) {
		RPCService.rpcResponsePort = rpcResponsePort;
	}

	@PreDestroy
	public void destroy() {
		if (serverSocket != null && !serverSocket.isClosed()) {
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
