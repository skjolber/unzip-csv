package com.github.skjolber.unzip;

import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Chunk splitter - determines chunk boundary
 * 
 */

public interface FileEntryChunkStreamHandler {

	FileChunkSplitter getFileChunkSplitter();

	void initialize(InputStream in, ThreadPoolExecutor executor) throws Exception;

	/**
	 * Handle a file entry chunk
	 * 
	 * @param in file binary stream
	 * @param chunkNumber chunk number (0 ... n) 
	 * @param fileEntryHandler entry handler
	 * @param executor work delegation executor
	 * @throws Exception if a problem occurs
	 */

	void handleChunk(InputStream in, int chunkNumber, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception;
	
}
