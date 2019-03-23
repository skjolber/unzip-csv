package com.github.skjolber.unzip;

public interface FileChunkSplitter {

	/**
	 * Get maximum length of next chunk. The chunk contents will be passed to {@link #getNextChunkIndex(byte[], int)} before the
	 * actual chunk is created.
	 * 
	 * @return number of bytes
	 */
	
	int getNextChunkLength();
	
	/**
	 * Determine the actual chunk length, by searching backwards
	 * 
	 * @param bytes content to search
	 * @param fromIndex the start index, for searching backwards
	 * @return chunk end index, or -1 if none
	 */
	
	int getNextChunkIndex(byte[] bytes, int fromIndex);

	
}
