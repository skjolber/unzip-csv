package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

public interface ChunkedFileEntryHandler extends FileEntryHandler {

	FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception;

}
