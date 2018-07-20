package com.github.skjolber.unzip;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public class UrlByteChannelCache {

	private static class Part {
		private volatile boolean downloaded = false;
		
		private ReentrantLock lock = new ReentrantLock(true);
		
		public void lock() {
			lock.lock();
		}
		public void unlock() {
			lock.unlock();
		}
		
		public void downloaded() {
			this.downloaded = true;
		}
		
		public boolean isDownloaded() {
			return downloaded;
		}
		public boolean isLocked() {
			return lock.isLocked();
		}
	}
	
	protected URL url;
	protected byte[] data;
	protected int chunkLength;
	protected Part[] parts;
	protected Semaphore connections;

	public UrlByteChannelCache(URL url, int chunkLength) {
		this(url, chunkLength, -1);
	}

	public UrlByteChannelCache(URL url, int chunkLength, int concurrentConnections) {
		this.url = url;
		this.chunkLength = chunkLength;
		if(concurrentConnections != -1) {
			connections = new Semaphore(concurrentConnections);
		}
	}
	
	public int getSize() throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		
		connection.setRequestMethod("HEAD");
		
		int responseCode = connection.getResponseCode();
		if(responseCode == 200) {
			return connection.getContentLength();
		} else {
			throw new IOException("Expected HTTP code 200, got " + responseCode);
		}
	}
	
	
	
    public void ensureContent(int position, int wanted) throws IOException {
    	int startIndex = position / chunkLength;
    	int endIndex = (position + wanted) / chunkLength;
    	
    	int length = endIndex - startIndex + 1;
    	
    	if(length == 1 && parts[startIndex].isDownloaded()) { // optimization for most common cause
    		return;
    	}
    	
    	int currentStartIndex = startIndex;
    	
    	do {
    		// greedy, request multiple parts per request
    		// find start
    		while(currentStartIndex < startIndex + length && parts[currentStartIndex].isDownloaded()) {
    			currentStartIndex++;
    		}

    		if(currentStartIndex == startIndex + length) {
    			break;
    		}

    		// find end
        	int currentLength = 1;
    		while(currentStartIndex + currentLength < startIndex + length && !parts[currentStartIndex + currentLength].isDownloaded()) {
    			currentLength++;
    		}
    		
    		if(currentLength == 0) {
    			break;
    		}
    		
    		boolean locked = false;
    		//  lock range
    		for(int i = currentStartIndex; i < currentStartIndex + currentLength; i++) {
    			if(parts[i].isLocked()) {
    				locked = true;
    			}
    			parts[i].lock();
    		}
    		try {
    			// if one of the parts was locked, the current start index and length is probably incorrect
    			if(locked) {
    				//  reevaluate current start and length
    				continue;
    			}

    			// restrict number of concurrent connections, if any
    			if(connections != null) {
    				connections.acquire();
    			}
    			
    			try {
					HttpURLConnection connection = (HttpURLConnection) url.openConnection();
					connection.setRequestProperty("Range", "bytes=" + (currentStartIndex * chunkLength) +"-" + (Math.min(data.length, (currentStartIndex + currentLength) * chunkLength) - 1));
					int responseCode = connection.getResponseCode();
					if(responseCode == 200 || responseCode == 206) {
						InputStream inputStream = connection.getInputStream();
						
						int offset = currentStartIndex * chunkLength;
						
						byte[] buffer = new byte[4096];
						
						int read;
						do {
							read = inputStream.read(buffer);
							if(read == -1) {
								break;
							}
							System.arraycopy(buffer, 0, data, offset, read);
							
							offset += read;
						} while(true);
					} else {
						throw new IOException("Expected HTTP code 200, got " + responseCode);
					} 
				} finally {
	    			if(connections != null) {
	    				connections.release();
	    			}
    			}
			} catch (InterruptedException e) {
				throw new IOException(e);
    		} finally {
        		//  release range
        		for(int i = currentStartIndex; i < currentStartIndex + currentLength; i++) {
        			parts[i].unlock();
        		}
    		}
			
			for(int i = currentStartIndex; i < currentStartIndex + currentLength; i++) {
				parts[i].downloaded();
			}
			
    		currentStartIndex = currentStartIndex + currentLength;
    	} while(currentStartIndex < startIndex + length);
    	
	}

	public int size() throws IOException {
		if(data == null) {
			synchronized(this) {
				if(data == null) {
					int size = getSize();
					data = new byte[size];
					
					int parts = size / chunkLength;
					if(size % chunkLength != 0) {
						parts++;
					}
					
					this.parts = new Part[parts];
					for(int i = 0; i < parts; i++) {
						this.parts[i] = new Part();
					}
				}
			}
		}

		return data.length;
	}

	public void put(ByteBuffer buf, int position, int wanted) throws IOException {
        ensureContent(position, wanted);
        buf.put(data, position, wanted);
	}
}
