package com.github.skjolber.unzip;

import java.io.IOException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

public class ByteArrayZipFileFactory implements ZipFileSource {

	protected byte[] buf;

	public ByteArrayZipFileFactory(byte[] buf) {
		this.buf = buf;
	}

	public ZipFile getZipFile() throws IOException {
		return new ZipFile(new SeekableInMemoryByteChannel(buf));
	}
	
	
}
