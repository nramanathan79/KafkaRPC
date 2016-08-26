package com.easyapp.kafka.rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestSocketServer {
	public static void main(String[] args) {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(4000);

			ExecutorService executor = Executors.newSingleThreadExecutor();
			Future<String> response = executor.submit(new RPCSocketServer(serverSocket));

			System.out.println(response.get());
			
			executor.shutdown();
		} catch (IOException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			try {
				if (serverSocket != null) {
					serverSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
