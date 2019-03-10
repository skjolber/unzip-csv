package com.github.skjolber.unzip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public class DefaultChunkedCsvFileEntryHandler implements ChunkedFileEntryHandler {

	@Override
	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}

	@Override
	public void beginFileCollection(String name) {
	}

	@Override
	public void beginFileEntry(String name) {
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
	}

	@Override
	public void endFileCollection(String name, ThreadPoolExecutor executor) {
	}


}
