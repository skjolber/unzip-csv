package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

public interface ChunkedFileEntryHandler extends FileEntryHandler {

	default FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}

}
