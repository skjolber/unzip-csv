package com.github.skjolber.unzip.csv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.stcsv.StaticCsvMapper2;
import com.github.skjolber.unzip.ChunkedFileEntryHandler;
import com.github.skjolber.unzip.FileChunkSplitter;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileEntryStreamHandler;
import com.github.skjolber.unzip.NewlineChunkSplitter;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractSesselTjonnaCsvFileEntryHandler implements ChunkedFileEntryHandler {

	protected abstract class StaticCsvMapperAdapter<T, D> implements StaticCsvMapper<T> {

		protected StaticCsvMapper2<T, D> staticCsvMapper2;
		
		public StaticCsvMapperAdapter(StaticCsvMapper2<T, D> staticCsvMapper2) {
			this.staticCsvMapper2 = staticCsvMapper2;
		}

		@Override
		public CsvReader<T> newInstance(Reader reader) {
			return staticCsvMapper2.newInstance(reader, createDelegate());
		}

		@Override
		public CsvReader<T> newInstance(Reader reader, char[] current, int offset, int length) {
			return staticCsvMapper2.newInstance(reader, current, offset, length, createDelegate());
		}

		protected abstract D createDelegate();
	}

	protected class CsvFileEntryStreamHandler implements FileEntryStreamHandler {

		protected final String name;
		
		public CsvFileEntryStreamHandler(String name) {
			super();
			this.name = name;
		}

		@Override
		public void handle(InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
			CsvReader reader = getCsvReader(name, in);
			
			CsvLineHandler csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
			if(csvLineHandler != null) {
				AbstractSesselTjonnaCsvFileEntryHandler.this.handle(csvLineHandler, name, reader, executor);
			} else {
				// ignore
			}
		}
	}

	protected class CsvFileEntryChunkStreamHandler implements FileEntryChunkStreamHandler {

		protected final String name;
		protected StaticCsvMapper mapper;
		
		public CsvFileEntryChunkStreamHandler(String name) {
			super();
			this.name = name;
		}

		@Override
		public FileChunkSplitter getFileChunkSplitter() {
			return AbstractSesselTjonnaCsvFileEntryHandler.this.getFileChunkSplitter(name);
		}

		@Override
		public void initialize(InputStream in, ThreadPoolExecutor executor) throws Exception {
			byte[] byteArray = getFirstLine(in).toByteArray();
			mapper = getStaticCsvMapper(name, byteArray);
		}

		@Override
		public void handleChunk(InputStream in, ThreadPoolExecutor executor, int chunkNumber) throws Exception {
			CsvLineHandler csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
			if(csvLineHandler != null) {
				CsvReader reader = mapper.newInstance(new InputStreamReader(in, StandardCharsets.UTF_8));

				handle(csvLineHandler, name, reader, executor);
			}
		}

		public ByteArrayOutputStream getFirstLine(InputStream in) throws IOException {
			// seek backward for a newline
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
			
			return out;
		}
		
	}

	protected CsvLineHandlerFactory csvLineHandlerFactory;

	public AbstractSesselTjonnaCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}

	protected void handle(CsvLineHandler csvLineHandler, String name, CsvReader reader, ThreadPoolExecutor executor) throws Exception {
		do {
			Object value = reader.next();
			if(value == null) {
				break;
			}
			csvLineHandler.handleLine(value);
		} while(true);
	}

	@Override
	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return new CsvFileEntryStreamHandler(name);
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return new CsvFileEntryChunkStreamHandler(name);
	}
	
	protected FileChunkSplitter getFileChunkSplitter(String name) {
		return new NewlineChunkSplitter();
	}

	protected abstract StaticCsvMapper getStaticCsvMapper(String name, byte[] byteArray) throws Exception;

	protected abstract CsvReader getCsvReader(String name, InputStream in) throws Exception;

}
