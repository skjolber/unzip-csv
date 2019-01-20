package com.github.skjolber.unzip;

import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Chunk splitter - determines chunk boundary
 * 
 */

public interface FileChunkSplitter {

	int getNextChunkIndex(byte[] bytes, int fromIndex);
	

	/**
	 * Handle a file entry chunk
	 * 
	 * @param name name of file
	 * @param size file size
	 * @param in file binary stream
	 * @param executor work delegation executor
	 * @param first true if the chunk is the first
	 * @throws Exception if a problem occurs
	 */

	void handleChunk(String name, long size, InputStream in, ThreadPoolExecutor executor, int chunkNumber) throws Exception;
	
}
