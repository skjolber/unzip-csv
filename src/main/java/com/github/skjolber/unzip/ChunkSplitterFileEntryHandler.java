package com.github.skjolber.unzip;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 
 * Read and split inputs into larger parts and queue their execution on the executor thread.
 * 
 */

public class ChunkSplitterFileEntryHandler implements FileEntryHandler {
	
	protected Map<String, FileEntryChunkState> parts = Collections.synchronizedMap(new HashMap<>());
	
	protected final int minimumChuckLength;
	protected final ChunkedFileEntryHandler delegate;
	
	/**
	 * Constructor.
	 * 
	 * @param minimumChuckLength global minimum number of bytes per segment
	 * @param delegate delegate for forwarding (whole or partial) streams. 
	 */
	
	public ChunkSplitterFileEntryHandler(int minimumChuckLength, ChunkedFileEntryHandler delegate) {
		this.minimumChuckLength = minimumChuckLength;
		this.delegate = delegate;
	}

	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		if(size > minimumChuckLength) {
			FileEntryChunkStreamHandler fileEntryChunkedStreamHandler = delegate.getFileEntryChunkedStreamHandler(name, size, executor);
			if(fileEntryChunkedStreamHandler != null) {
				FileEntryChunkState fileEntryState = new FileEntryChunkState(name, this, executor);
				parts.put(name, fileEntryState);
				return new FileEntryChunkStreamHandlerAdapter(fileEntryState, fileEntryChunkedStreamHandler, executor);
			} else {
				return delegate.getFileEntryStreamHandler(name, size, executor);
			}
		} else {
			return delegate.getFileEntryStreamHandler(name, size, executor);
		}
	}

	@Override
	public void beginFileEntry(String name) {
		delegate.beginFileEntry(name);
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		FileEntryChunkState fileEntryState = parts.remove(name);
		if(fileEntryState != null) {
			fileEntryState.ended();
		} else {
			delegate.endFileEntry(name, executor);
		}
	}

	@Override
	public void beginFileCollection(String name) {
		delegate.beginFileCollection(name);
	}

	@Override
	public void endFileCollection(String name, ThreadPoolExecutor executor) {
		delegate.endFileCollection(name, executor);
	}


}
