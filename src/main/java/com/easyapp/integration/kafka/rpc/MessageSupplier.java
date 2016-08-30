package com.easyapp.integration.kafka.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.easyapp.integration.kafka.util.Pair;

public class MessageSupplier implements Callable<Void>, Supplier<Pair<String, String>> {
	private final Socket clientSocket;
	private final RPCService rpcService;

	public MessageSupplier(final Socket clientSocket, final RPCService rpcService) {
		this.clientSocket = clientSocket;
		this.rpcService = rpcService;
	}

	@Override
	public Void call() throws Exception {
		Pair<String, String> message = get();

		rpcService.getQueue(message.getKey()).put(message.getValue());

		return null;
	}

	@Override
	public Pair<String, String> get() {
		BufferedReader reader = null;
		PrintWriter writer = null;
		String key = null;
		String value = null;

		try {
			// Read until EOF
			boolean EOF = false;
			reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			StringBuilder builder = new StringBuilder();

			while (!EOF) {
				String line = reader.readLine();

				if (line == null || ("EOF").equals(line)) {
					EOF = true;
				} else if (line.startsWith("KEY:")) {
					builder.append(line.substring(4));
				} else if (line.startsWith("VALUE:")) {
					key = builder.toString();
					builder = new StringBuilder();
					builder.append(line.substring(6));
				} else {
					builder.append(line);
				}
			}

			value = builder.toString();

			writer = new PrintWriter(clientSocket.getOutputStream(), true);
			writer.println("OK");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				// Close Streams and Socket
				if (writer != null) {
					writer.close();
				}

				if (reader != null) {
					reader.close();
				}

				if (clientSocket != null && !clientSocket.isClosed()) {
					clientSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return new Pair<>(key, value);
	}
}
