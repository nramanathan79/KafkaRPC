package com.easyapp.integration.kafka.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.easyapp.integration.kafka.util.KafkaProperties;

@Service
public class RPCService {
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private final Map<String, BlockingQueue<String>> rpcRegistry = new ConcurrentHashMap<>();

	private ServerSocket serverSocket;
	private InetAddress rpcResponseHost;
	private int rpcResponsePort;
	private long timeoutMillis;

	public RPCService() {
		final Properties rpcProperties = KafkaProperties.getKafkaRPCProperties();

		try {
			final String responseHost = rpcProperties.getProperty("response.host");
			rpcResponseHost = InetAddress.getByName(responseHost);
		} catch (UnknownHostException e) {
			try {
			rpcResponseHost = InetAddress.getLocalHost();
			}
			catch (UnknownHostException uhe) {
				uhe.printStackTrace();
			}
		}
		
		try {
			rpcResponsePort = Integer.parseInt(rpcProperties.getProperty("response.port"));
		} catch (Exception e) {
			rpcResponsePort = 11111;
		}

		try {
			timeoutMillis = Long.parseLong(rpcProperties.getProperty("response.timeout.ms"));
		} catch (Exception e) {
			timeoutMillis = 10000;
		}

		try {
			serverSocket = new ServerSocket(rpcResponsePort);
			executor.submit(new RPCSocketServer(serverSocket, this, timeoutMillis));
		} catch (IOException e) {
			e.printStackTrace();
			serverSocket = null;
		}
	}

	public InetAddress getResponseHost() {
		return rpcResponseHost;
	}
	
	public int getRPCResponsePort() {
		return rpcResponsePort;
	}

	public long getTimeoutMillis() {
		return timeoutMillis;
	}

	public void addToRegistry(String key) {
		rpcRegistry.put(key, new LinkedBlockingQueue<>());
	}

	public BlockingQueue<String> getQueue(String key) {
		return rpcRegistry.get(key);
	}

	public void removeFromRegistry(String key) {
		rpcRegistry.remove(key);
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
