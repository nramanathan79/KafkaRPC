package com.easyapp.kafka.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

public class RPCSocketServer implements Callable<String> {
	private final ServerSocket serverSocket;

	public RPCSocketServer(final ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	@Override
	public String call() throws Exception {
		Socket clientSocket = null;
		BufferedReader reader = null;
		StringBuilder builder = new StringBuilder();

		try {
			// Accept a connection from client
			clientSocket = serverSocket.accept();

			// Read until EOF
			boolean EOF = false;
			reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

			while (!EOF && !Thread.currentThread().isInterrupted()) {
				String line = reader.readLine();

				if (line == null || ("EOF").equals(line)) {
					EOF = true;
				} else {
					builder.append(line);
				}
			}

			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
			out.println("OK");
			out.close();
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

		return builder.toString();
	}
}
