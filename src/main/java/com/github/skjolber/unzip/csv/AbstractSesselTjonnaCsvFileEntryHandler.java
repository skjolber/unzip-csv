package com.github.skjolber.unzip.csv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.unzip.ChunkedFileEntryHandler;
import com.github.skjolber.unzip.FileChunkSplitter;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileEntryStreamHandler;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractSesselTjonnaCsvFileEntryHandler implements ChunkedFileEntryHandler {

	protected static abstract class AbstractCsvFileEntryStreamHandler<T> implements FileEntryStreamHandler {

		protected final String name;
		protected final CsvLineHandlerFactory csvLineHandlerFactory;
		
		public AbstractCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory) {
			super();
			this.name = name;
			this.csvLineHandlerFactory = csvLineHandlerFactory;
		}

		@Override
		public void handle(InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
			CsvLineHandler<T> handler = csvLineHandlerFactory.getHandler(name, executor);
			if(handler != null) {
				handle(createCsvReader(in), handler, executor);
			}
		}

		protected void handle(CsvReader<T> reader, CsvLineHandler<T> handler, ThreadPoolExecutor executor) throws Exception {
			do {
				T value = reader.next();
				if(value == null) {
					break;
				}
				handler.handleLine(value);
			} while(true);
		}

		protected abstract CsvReader<T> createCsvReader(InputStream in) throws Exception;

	}

	protected static abstract class AbstractCsvFileEntryChunkStreamHandler<T> implements FileEntryChunkStreamHandler {

		protected final String name;
		protected StaticCsvMapper<T> mapper;
		protected Charset charset;
		protected FileChunkSplitter fileChunkSplitter;
		protected final CsvLineHandlerFactory csvLineHandlerFactory;

		public AbstractCsvFileEntryChunkStreamHandler(String name, Charset charset, FileChunkSplitter fileChunkSplitter, CsvLineHandlerFactory csvLineHandlerFactory) {
			super();
			this.name = name;
			this.charset = charset;
			this.fileChunkSplitter = fileChunkSplitter;
			this.csvLineHandlerFactory = csvLineHandlerFactory;
		}

		@Override
		public FileChunkSplitter getFileChunkSplitter() {
			return fileChunkSplitter;
		}

		@Override
		public void initialize(InputStream in, ThreadPoolExecutor executor) throws Exception {
			mapper = createStaticCsvMapper(getFirstLine(in));
		}

		@Override
		public void handleChunk(InputStream in, ThreadPoolExecutor executor, int chunkNumber) throws Exception {
			CsvLineHandler<T> handler = csvLineHandlerFactory.getHandler(name, executor);
			if(handler != null) {
				handle(mapper.newInstance(new InputStreamReader(in, charset)), handler, executor);
			}
		}

		public String getFirstLine(InputStream in) throws IOException {
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			int read;
			do {
				read = in.read();
				if(read == -1) {
					throw new IllegalArgumentException();
				}
				out.write(read);
				if(read == '\n') {
					break;
				}
			} while(true);
			
			return new String(out.toByteArray(), charset);
		}
		
		protected abstract StaticCsvMapper<T> createStaticCsvMapper(String firstLine) throws Exception;

		protected void handle(CsvReader<T> reader, CsvLineHandler<T> handler, ThreadPoolExecutor executor) throws Exception {
			do {
				T value = reader.next();
				if(value == null) {
					break;
				}
				handler.handleLine(value);
			} while(true);
		}

	}

	@Override
	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}


}
