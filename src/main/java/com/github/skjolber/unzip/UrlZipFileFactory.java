package com.github.skjolber.unzip;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.compress.archivers.zip.ZipFile;

public class UrlZipFileFactory implements ZipFileSource {

	protected UrlByteChannelCache cache;
	
	public UrlZipFileFactory(URL url, int chunkLength) {
		this(url, chunkLength, -1);
	}

	public UrlZipFileFactory(URL url, int chunkLength, int concurrentConnections) {
		cache = new UrlByteChannelCache(url, chunkLength, concurrentConnections);
	}

	public ZipFile getZipFile() throws IOException {
		return new ZipFile(new UrlSeekableByteChannel(cache));
	}
	
	public int size() throws IOException {
		return cache.getSize();
	}

}
