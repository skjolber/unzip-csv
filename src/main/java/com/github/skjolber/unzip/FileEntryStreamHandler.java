package com.github.skjolber.unzip;

import java.io.InputStream;

public interface FileEntryStreamHandler {

	/**
	 * Handle a file entry
	 * 
	 * @param in file binary stream
	 * @param consume if true, the stream must be consumed in the current thread (not delegated to executor)
	 * @throws Exception if a problem occurs
	 */
	void handle(InputStream in, boolean consume) throws Exception;

}
