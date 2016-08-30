package com.easyapp.integration.kafka.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Optional;

import com.easyapp.integration.kafka.bean.RPCMessageMetadata;

public class RPCSocketClient {

	public static Optional<String> send(RPCMessageMetadata messageMetadata, String response) {
		Socket clientSocket = null;
		BufferedReader reader = null;
		Optional<String> acknowledgement = Optional.empty();

		try {
			// Connecting to the RPC producer
			clientSocket = new Socket(messageMetadata.getReplyHost(), messageMetadata.getReplyPort());

			// Write the response
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
			out.println("KEY:" + messageMetadata.getKey());
			out.println("VALUE:" + response);
			out.println("EOF");

			// Wait for an OK
			boolean ok = false;
			reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

			while (!ok) {
				String line = reader.readLine();

				if (line == null || ("OK").equals(line)) {
					ok = true;
					acknowledgement = Optional.ofNullable(line);
				}
			}

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
		
		return acknowledgement;
	}
}
