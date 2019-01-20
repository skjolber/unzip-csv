package com.github.skjolber.unzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * Read and split inputs into larger parts and queue their execution on the executor thread.
 * 
 */

public class ChunkSplitterFileEntryHandler implements ChunkedFileEntryHandler {

	protected static class FileEntryState {
		private AtomicInteger count = new AtomicInteger(0);
		protected volatile boolean ended = false;
		protected volatile boolean notified = false;
		
		public int increment() {
			return count.incrementAndGet();
		}

		public int decrement() {
			return count.decrementAndGet();
		}
		
		public int get() {
			return count.get();
		}

		public void ended() {
			ended = true;
		}
		
		public boolean isEnded() {
			return ended;
		}
		
		public void notified() {
			notified = true;
		}
		
		public boolean isNotified() {
			return notified;
		}
		
	}

	protected Map<String, FileEntryState> parts = Collections.synchronizedMap(new HashMap<>());
	
	/**
	 * Notify when processing is performed
	 * 
	 * @param name name of file processed
	 * @param executor thread pool executor (for queuing additional post-processing)
	 */

	protected final int chuckLength; // effective length depends on line lengths
	protected final ChunkedFileEntryHandler delegate;
	
	/**
	 * Constructor.
	 * 
	 * @param chuckLength number of bytes per segment
	 * @param delegate delegate for forwarding (whole or partial) streams. 
	 */
	
	public ChunkSplitterFileEntryHandler(int chuckLength, ChunkedFileEntryHandler delegate) {
		super();
		this.chuckLength = chuckLength;
		this.delegate = delegate;
	}

	public void handle(final String name, long size, InputStream in, final ThreadPoolExecutor executor, boolean consume) throws Exception {
		if(size > chuckLength) {
			FileChunkSplitter splitter = delegate.getFileEntryChunkSplitter(name, size, in, executor);
			if(splitter != null) {
				FileEntryState fileEntryState = new FileEntryState();
				parts.put(name, fileEntryState);

				byte[] buffer = new byte[Math.min(8192 * 16, chuckLength)];
	
				ByteArrayOutputStream bout = new ByteArrayOutputStream(chuckLength);
				
				int chunkNumber = 0;
				
				while(true) {
					
					int remaining = chuckLength;
					
					int read;
					do {
						read = in.read(buffer, 0, Math.min(buffer.length, remaining));
	
						if(read == -1) {
							break;
						}
						
						remaining -= read;
	
						bout.write(buffer, 0, read);
					} while(remaining > 0);
					
					final byte[] byteArray = bout.toByteArray();
					
					if(read == -1) {
						// end of file
						fileEntryState.increment();
						
						handleChunk(splitter, fileEntryState, name, byteArray.length, new ByteArrayInputStream(byteArray), executor, false, chunkNumber);
						
						break;
					} else {
						int index = splitter.getNextChunkIndex(byteArray, byteArray.length - 1);
	
						if(index == -1) {
							throw new IllegalArgumentException("No newline found in chunk size " + byteArray.length);
						}
						fileEntryState.increment();

						handleChunk(splitter, fileEntryState, name, size, new ByteArrayInputStream(byteArray, 0, index - 1), executor, false, chunkNumber);
	
						// reuse buffer
						bout.reset();
						// write tail
						if(index + 1 < byteArray.length) {
							bout.write(byteArray, index + 1, byteArray.length - index - 1);
						}
						
						chunkNumber++;
					}
				}
			} else {
				delegate.handle(name, size, in, executor, consume);
			}
		} else {
			delegate.handle(name, size, in, executor, consume);
		}
	}

	@Override
	public void beginFileEntry(String name) {
		delegate.beginFileEntry(name);
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		FileEntryState fileEntryState = parts.get(name);
		if(fileEntryState != null) {
			fileEntryState.ended();
			notifyEndFileEntry(name, fileEntryState, executor);
		} else {
			delegate.endFileEntry(name, executor);
		}
	}

	protected void notifyEndFileEntry(String name, FileEntryState fileEntryState, ThreadPoolExecutor executor) {
		synchronized (fileEntryState) {
			if(fileEntryState.get() == 0 && fileEntryState.isEnded() && !fileEntryState.isNotified()) {
				fileEntryState.notified();
				
				delegate.endFileEntry(name, executor);
				
				parts.remove(name);
			}
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

	protected void handleChunk(FileChunkSplitter splitter, FileEntryState fileEntryState, String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume, int chunkNumber) throws Exception {
		executor.execute(new Runnable() {
			public void run() {
				try {
					splitter.handleChunk(name, size, in, executor, chunkNumber);

					fileEntryState.decrement();
					
					notifyEndFileEntry(name, fileEntryState, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	@Override
	public FileChunkSplitter getFileEntryChunkSplitter(String name, long size, InputStream in, ThreadPoolExecutor executor) throws Exception {
		return delegate.getFileEntryChunkSplitter(name, size, in, executor);
	}

}
