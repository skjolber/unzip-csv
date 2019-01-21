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
	 * @param executor work delegation executor
	 * @param first true if the chunk is the first
	 * @throws Exception if a problem occurs
	 */

	void handleChunk(InputStream in, ThreadPoolExecutor executor, int chunkNumber) throws Exception;
	
}
