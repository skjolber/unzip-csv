package com.github.skjolber.unzip;

public interface FileChunkSplitter {

	int getNextChunkIndex(byte[] bytes, int fromIndex);

}
