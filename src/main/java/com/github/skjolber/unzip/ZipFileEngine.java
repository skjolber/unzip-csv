package com.github.skjolber.unzip;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

public class ZipFileEngine implements UncaughtExceptionHandler {

	protected FileEntryHandler handler;
	protected ThreadPoolExecutor executor;
	protected volatile Throwable uncaughtException = null;
	
	public ZipFileEngine(FileEntryHandler handler) {
		this(handler, Runtime.getRuntime().availableProcessors());
	}

	public ZipFileEngine(FileEntryHandler handler, int threads) {
		this(handler, (ThreadPoolExecutor) Executors.newFixedThreadPool(threads));
	}

	public ZipFileEngine(FileEntryHandler handler, ThreadPoolExecutor executor) {
		super();
		this.handler = handler;
		this.executor = executor;
	}

	public boolean handle(ZipFileSource source) throws IOException {
		return handle(source, null);
	}

	public boolean handle(ZipFileSource source, List<String> files) throws IOException {
		AtomicInteger counter = new AtomicInteger();
		
		List<String> targetFiles;
		if(files == null) {
			ZipFile zipFile = source.getZipFile();
			
			targetFiles = new ArrayList<String>(64);
			
			Enumeration<ZipArchiveEntry> entriesInPhysicalOrder = zipFile.getEntriesInPhysicalOrder();
			while(entriesInPhysicalOrder.hasMoreElements()) {
				ZipArchiveEntry nextElement = entriesInPhysicalOrder.nextElement();
				targetFiles.add(nextElement.getName());
			}
			zipFile.close();
		} else {
			targetFiles = files;
		}
		
		// wrap exception handler to detect errors
		ThreadFactory threadFactory = executor.getThreadFactory();
		executor.setThreadFactory(new UncaughtExceptionHandlerThreadFactory(threadFactory, this));
		try {
			handler.beginFileCollection(null);
			for(int i = 0; i < executor.getMaximumPoolSize(); i++) {
				ZipFile z = source.getZipFile();
				executor.execute(new ZipFileProcessor(counter, targetFiles, z, handler, executor));
			}
			
			while (executor.getActiveCount() > 0 || !executor.getQueue().isEmpty()) {
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					break;
				}
	        }
			handler.endFileCollection(null, executor);
			
			while (executor.getActiveCount() > 0 || !executor.getQueue().isEmpty()) {
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					break;
				}
	        }
			
			return uncaughtException == null;
		} finally {
			executor.setThreadFactory(threadFactory);
		}
	}
	
	public void close() {
		executor.shutdown();
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		this.uncaughtException = e;
	}
	
	public void reset() {
		uncaughtException = null;
	}
	
	public Throwable getUncaughtException() {
		return uncaughtException;
	}
	
	public ThreadPoolExecutor getExecutor() {
		return executor;
	}
}
