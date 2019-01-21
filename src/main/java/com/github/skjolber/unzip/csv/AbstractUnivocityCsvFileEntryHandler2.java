package com.github.skjolber.unzip.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.AbstractCsvMapper;
import com.github.skjolber.stcsv.AbstractCsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.unzip.ChunkedFileEntryHandler;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.NewlineChunkSplitter;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractUnivocityCsvFileEntryHandler2 implements ChunkedFileEntryHandler {

	protected CsvLineHandlerFactory csvLineHandlerFactory;

	public AbstractUnivocityCsvFileEntryHandler2(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}

	@SuppressWarnings("rawtypes")
	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		AbstractCsvReader reader = createCsvReader(in, name);

		CsvLineHandler csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			handle(csvLineHandler, name, reader, executor);
		} else {
			// ignore
		}
	}

	protected <T> void handle(CsvLineHandler<T> csvLineHandler, String name, AbstractCsvReader<T> reader, ThreadPoolExecutor executor) throws Exception {
		do {
			T value = reader.next();
			if(value == null) {
				break;
			}
			csvLineHandler.handleLine(value);
		} while(true);
	}

	protected abstract AbstractCsvReader createCsvReader(InputStream in, String name);

	public ByteArrayOutputStream getFirstLine(InputStream in) throws IOException {
		// seek backward for a newline
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		int read;
		do {
			read = in.read();
			if(read == -1) {
				throw new IllegalArgumentException();
			}
			if(read == '\n') {
				break;
			}
			out.write(read);
		} while(true);
		
		return out;
	}

	protected class CsvFileChunkSplitter extends NewlineChunkSplitter {

		private final StaticCsvMapper<?> reader;
		
		public CsvFileChunkSplitter(String[] names) {
			this.names = names;
		}

		@Override
		public void handleChunk(String name, long size, InputStream in, ThreadPoolExecutor executor, int chunkNumber) throws Exception {
			CsvLineHandler<Map<String, String>> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
			if(csvLineHandler != null) {
				CsvParser reader = createCsvParser(in);

				handle(csvLineHandler, name, reader, names, executor);
			}
		}
		
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkStreamHandler(String name, long size, InputStream in, ThreadPoolExecutor executor) throws Exception {

		StaticCsvMapper<?> reader = createStaticCsvMapper(in, name);

		CsvLineHandler csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			handle(csvLineHandler, name, reader, executor);
		} else {
			// ignore
		}

		
		
		return null;
	}

	private StaticCsvMapper<?> createStaticCsvMapper(InputStream in, String name) {
		// TODO Auto-generated method stub
		return null;
	}
}
