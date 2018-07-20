package com.github.skjolber.unzip;

import java.io.File;
import java.io.IOException;

import org.apache.commons.compress.archivers.zip.ZipFile;

public class FileZipFileFactory implements ZipFileSource {

	protected File file;

	public FileZipFileFactory(File file) {
		this.file = file;
	}

	public ZipFile getZipFile() throws IOException {
		return new ZipFile(file);
	}
	
}
