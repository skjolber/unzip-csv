	package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.skjolber.unzip.ChunkedFileEntryHandler;
import com.github.skjolber.unzip.FileEntryHandler;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractCsvFileEntryHandler<T> implements ChunkedFileEntryHandler {

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
	
	public AbstractCsvFileEntryHandler() {
	}

	public abstract void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception;

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		FileEntryState fileEntryState = parts.get(name);
		fileEntryState.ended();
		notifyEndFileEntry(name, fileEntryState, executor);
	}

	protected void notifyEndFileEntry(String name, FileEntryState fileEntryState, ThreadPoolExecutor executor) {
		synchronized (fileEntryState) {
			if(fileEntryState.get() == 0 && fileEntryState.isEnded() && !fileEntryState.isNotified()) {
				fileEntryState.notified();
				
				endFileEntryProcessing(name, executor);
				
				parts.remove(name);
			}
			
		}
	}
	/**
	 * Notify when processing is performed
	 * 
	 * @param name name of file processed
	 * @param executor thread pool executor (for queuing additional post-processing)
	 */

	protected abstract void endFileEntryProcessing(String name, ThreadPoolExecutor executor);


	@Override
	public void beginFileEntry(String name) {
		parts.put(name, new FileEntryState());
	}
}
