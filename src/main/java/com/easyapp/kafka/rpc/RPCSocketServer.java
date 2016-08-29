package com.easyapp.kafka.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

public class RPCSocketServer implements Callable<Void> {
	private final ServerSocket serverSocket;

	public RPCSocketServer(final ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	@Override
	public Void call() throws Exception {
		// Run in this thread a server listening to various messages.
		while (!Thread.currentThread().isInterrupted()) {
			Socket clientSocket = null;
			BufferedReader reader = null;

			try {
				// Accept the socket connection from client
				clientSocket = serverSocket.accept();

				// Read until EOF
				boolean EOF = false;
				reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				StringBuilder builder = new StringBuilder();

				while (!EOF) {
					String line = reader.readLine();

					if (line == null || ("EOF").equals(line)) {
						EOF = true;
					} else {
						builder.append(line);
					}
				}

				PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
				out.println("OK");

				// Close Streams and Socket
				out.close();
				reader.close();
				clientSocket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (reader != null) {
						reader.close();
					}

					if (clientSocket != null) {
						clientSocket.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return null;
	}
}
