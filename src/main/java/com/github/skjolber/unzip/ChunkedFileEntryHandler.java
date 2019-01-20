package com.github.skjolber.unzip;

import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

public interface ChunkedFileEntryHandler extends FileEntryHandler {

	FileChunkSplitter getFileEntryChunkSplitter(final String name, long size, InputStream in, ThreadPoolExecutor executor) throws Exception;

}
