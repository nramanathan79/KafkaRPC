package com.easyapp.kafka.rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.easyapp.kafka.util.KafkaProperties;

@Service
public class RPCService {
	private final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();
	private int rpcResponsePort;
	private ServerSocket serverSocket = null;
	
	public RPCService() {
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

	public ServerSocket getRPCServerSocket() {
		return serverSocket;
	}

	public Properties getRPCProperties() {
		return rpcProperties;
	}

	public int getRPCResponsePort() {
		return rpcResponsePort;
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
