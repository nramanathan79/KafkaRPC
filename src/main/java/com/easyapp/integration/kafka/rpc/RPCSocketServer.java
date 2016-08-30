package com.easyapp.integration.kafka.rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RPCSocketServer implements Callable<Void> {
	private final ServerSocket serverSocket;
	private final RPCService rpcService;
	private final long timeoutMillis;

	public RPCSocketServer(final ServerSocket serverSocket, final RPCService rpcService, final long timeoutMillis) {
		this.serverSocket = serverSocket;
		this.rpcService = rpcService;
		this.timeoutMillis = timeoutMillis;
	}

	@Override
	public Void call() {
		ExecutorService executor = Executors.newCachedThreadPool();
		Socket clientSocket = null;

		try {
			// Run in this thread a server listening to various messages.
			while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
				try {
					// Accept the socket connection from client
					clientSocket = serverSocket.accept();

					// Run the socket connection on its own thread
					executor.submit(new MessageSupplier(clientSocket, rpcService, timeoutMillis));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} finally {
			try {
				if (clientSocket != null && !clientSocket.isClosed()) {
					clientSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			executor.shutdown();
		}

		return null;
	}
}
