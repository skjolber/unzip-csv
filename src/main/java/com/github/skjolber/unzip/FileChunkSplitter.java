package com.github.skjolber.unzip;

/**
 * Chunk splitter - determines chunk boundary
 * 
 */

public interface FileChunkSplitter {

	int getChunkSplitIndex(byte[] bytes, int fromIndex);
}
