package com.easyapp.kafka.rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.easyapp.kafka.util.KafkaProperties;

@Service
public class RPCService {
	private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = Executors.defaultThreadFactory().newThread(r);
			t.setDaemon(true);
			return t;
		}
	});

	private ServerSocket serverSocket;
	private int rpcResponsePort;

	public RPCService() {
		final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();

		try {
			rpcResponsePort = Integer.parseInt(rpcProperties.getProperty("response.port"));
		} catch (Exception e) {
			rpcResponsePort = 11111;
		}

		try {
			serverSocket = new ServerSocket(rpcResponsePort);
			executor.submit(new RPCSocketServer(serverSocket));
		} catch (IOException e) {
			e.printStackTrace();
			serverSocket = null;
		}
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
		
		executor.shutdown();
	}
}
