package com.github.skjolber.unzip;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

public class ZipFileProcessor implements Runnable {

	protected AtomicInteger counter;
	protected ZipFile zipFile;
	protected FileEntryHandler handler;
	protected List<String> files;
	protected ThreadPoolExecutor executor;
	
	private boolean closed = false;
	
	public ZipFileProcessor(AtomicInteger counter, List<String> files, ZipFile zipFile, FileEntryHandler handler, ThreadPoolExecutor executor) {
		this.counter = counter;
		this.zipFile = zipFile;
		this.handler = handler;
		this.files = files;
		this.executor = executor;
	}

	public void run() {
		do {
			int number = counter.getAndIncrement();
			if(number < files.size()) {
				String name = files.get(number);
				try {
					ZipArchiveEntry ze = zipFile.getEntry(name);
					if(ze != null) {
						InputStream zin = zipFile.getInputStream(ze);
						try {
							handler.beginFileEntry(name);
							handler.handle(name, ze.getSize(), zin, executor, true);
							handler.endFileEntry(name, executor);
						} catch (Exception e) {
							throw new RuntimeException(e);
						} finally {
							zin.close();
						}
					} else {
						// TOOD not found
					}
				} catch (IOException e) {
					throw new IllegalArgumentException(e);
				}
			} else {
				try {
					zipFile.close();
				} catch (IOException e) {
					throw new RuntimeException();
				}

				// work finished
				break;
			}
			
		} while(!closed);
	}

	public void close() {
		this.closed = true;
	}
	

	
}
