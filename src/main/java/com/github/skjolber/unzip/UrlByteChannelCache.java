package com.github.skjolber.unzip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Byte-array cache for remote HTTP content. Sees the remote content as a number of segments which are
 * locked and downloaded individually. 
 */

public class UrlByteChannelCache {

	private static class Part {
		private volatile byte[] content;
		
		private final ReentrantLock lock = new ReentrantLock(true);
		
		public void lock() {
			lock.lock();
		}
		public void unlock() {
			lock.unlock();
		}
		
		public void downloaded(byte[] content) {
			this.content = content;
		}
		
		public boolean isDownloaded() {
			return content != null;
		}
		public boolean isLocked() {
			return lock.isLocked();
		}
	}
	
	protected volatile int size = -1;
	protected URL url;
	protected int chunkLength;
	protected Part[] parts;
	protected Semaphore concurrentConnections;

	public UrlByteChannelCache(URL url, int chunkLength) {
		this(url, chunkLength, -1);
	}

	public UrlByteChannelCache(URL url, int chunkLength, int numberOfConcurrentConnections) {
		this.url = url;
		this.chunkLength = chunkLength;
		if(numberOfConcurrentConnections != -1) {
			this.concurrentConnections = new Semaphore(numberOfConcurrentConnections);
		}
	}
	
	protected int getSize() throws IOException {
		HttpURLConnection connection = openConnection();
		
		connection.setRequestMethod("HEAD");
		
		int responseCode = connection.getResponseCode();
		if(responseCode == 200) {
			return connection.getContentLength();
		} else {
			throw new IOException("Expected HTTP code 200, got " + responseCode);
		}
	}
	
    public void ensureContentBytes(int position, int wanted) throws IOException {
    	int startIndex = position / chunkLength;
    	int endIndex = (position + wanted) / chunkLength;
    	
    	ensureContentIndex(startIndex, endIndex);
    }
	
    public void ensureContentIndex(int startIndex, int endIndex) throws IOException {
    	
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
    			if(concurrentConnections != null) {
    				concurrentConnections.acquire();
    			}
    			
    			try {
					HttpURLConnection connection = openConnection();
					connection.setRequestProperty("Range", "bytes=" + (currentStartIndex * chunkLength) +"-" + (Math.min(size, (currentStartIndex + currentLength) * chunkLength) - 1));
					int responseCode = connection.getResponseCode();
					if(responseCode == 200 || responseCode == 206) {
						InputStream inputStream = connection.getInputStream();

						// directly create output byte arrays on-the-go
						byte[] buffer = new byte[4096];
						
		        		for(int i = currentStartIndex; i < currentStartIndex + currentLength; i++) {
		        			byte[] partContent = new byte[Math.min(chunkLength, size - currentStartIndex * chunkLength)];

		        			int index = 0;
		        			
							int read;
							do {
								read = inputStream.read(buffer, 0, Math.min(partContent.length - index, buffer.length));
								if(read == -1) {
									break;
								}
								
								System.arraycopy(buffer, 0, partContent, index, read);
								
								index += read;
							} while(index < partContent.length);
		        			
		        			parts[i].downloaded(partContent);
		        		}
						
					} else {
						throw new IOException("Expected HTTP code 200, got " + responseCode);
					} 
				} finally {
	    			if(concurrentConnections != null) {
	    				concurrentConnections.release();
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
			
    		currentStartIndex = currentStartIndex + currentLength;
    	} while(currentStartIndex < startIndex + length);
    	
	}

	protected HttpURLConnection openConnection() throws IOException {
		return (HttpURLConnection) url.openConnection();
	}

	public int size() throws IOException {
		if(size == -1) {
			synchronized(this) {
				if(size == -1) {
					size = getSize();
					
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

		return size;
	}

	public int put(ByteBuffer buf, int position, int wanted) throws IOException {
    	int startIndex = position / chunkLength;
    	int endIndex = (position + wanted) / chunkLength;

        ensureContentIndex(startIndex, endIndex);
    	
		int offest = position - startIndex * chunkLength;
		int length = Math.min(wanted, parts[startIndex].content.length - offest);
		
		buf.put(parts[startIndex].content, offest, length);
		
		return length;
	}
		
}
