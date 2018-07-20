package com.github.skjolber.unzip;

import java.io.IOException;

import org.apache.commons.compress.archivers.zip.ZipFile;

public interface ZipFileSource {

	ZipFile getZipFile() throws IOException;
}
