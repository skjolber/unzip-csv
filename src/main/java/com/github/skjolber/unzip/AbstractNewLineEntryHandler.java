package com.github.skjolber.unzip;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AbstractNewLineEntryHandler implements FileEntryHandler {

	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
		
		try {
			do {
				String line = reader.readLine();
				if(line == null) {
					break;
				}
				handleNewLine(line);
			} while(true);
		} finally {
			reader.close();
		}
	}

	protected abstract void handleNewLine(String line);

}
