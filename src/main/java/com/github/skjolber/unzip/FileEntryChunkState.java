package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class FileEntryChunkState implements FileEntryHandler {
	
	private AtomicInteger count = new AtomicInteger(0);
	protected volatile boolean ended = false;
	protected volatile boolean notified = false;
	
	protected String name;
	protected FileEntryHandler fileEntryHandler;
	protected ThreadPoolExecutor executor;

	public FileEntryChunkState(String name, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) {
		this.name = name;
		this.fileEntryHandler = fileEntryHandler;
		this.executor = executor;
	}

	public int increment() {
		return count.incrementAndGet();
	}

	public int decrement() {
		synchronized (this) {
			try {
				return count.decrementAndGet();
			} finally {
				endFileEntry();
			}
		}
	}
	
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		decrement();
	}
	
	private void endFileEntry() {
		if(get() == 0 && isEnded() && !isNotified()) {
			notified();
			
			fileEntryHandler.endFileEntry(name, executor);
		}
	}
	
	public int get() {
		return count.get();
	}

	public void ended() {
		synchronized (this) {
			try {
				ended = true;
			} finally {
				endFileEntry();
			}
		}
	}
	
	public boolean isEnded() {
		return ended;
	}
	
	public void notified() {
		synchronized (this) {
			try {
				notified = true;
			} finally {
				endFileEntry();
			}
		}
	}
	
	public boolean isNotified() {
		return notified;
	}
	
	
}
