package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileEntryStreamHandler;

public abstract class AbstractSesselTjonnaCsvFileEntryStreamHandler<T> implements FileEntryStreamHandler {

	protected final String name;
	protected final CsvLineHandlerFactory csvLineHandlerFactory;
	protected final long size;
	protected final boolean parallel;
	protected final FileEntryHandler fileEntryHandler;
	protected final ThreadPoolExecutor executor;

	public AbstractSesselTjonnaCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, long size, FileEntryHandler delegate, ThreadPoolExecutor executor) {
		this(name, csvLineHandlerFactory, size, false, delegate, executor);
	}

	public AbstractSesselTjonnaCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, long size, boolean parallel, FileEntryHandler delegate, ThreadPoolExecutor executor) {
		super();
		this.name = name;
		this.csvLineHandlerFactory = csvLineHandlerFactory;
		this.size = size;
		this.parallel = parallel;
		this.fileEntryHandler = delegate;
		this.executor = executor;
	}

	@Override
	public void handle(InputStream in, boolean consume) throws Exception {
		CsvLineHandler<T> handler = csvLineHandlerFactory.getHandler(name, executor);
		if(handler != null) {
			Reader reader = createReader(in);
			if(parallel && executor.getCorePoolSize() > 1) {
				// handle the reader in this thread, queue the csv parser itself in another job
				ParallelReader parallelReader = new ParallelReader(reader, Math.min((int)size, 64 * 1024 * 1024));
				handleParalell(parallelReader, handler, fileEntryHandler, executor);
				parallelReader.getRunnable().run(); // consume input now
			} else {
				handle(createCsvReader(reader, executor), handler, fileEntryHandler, executor);
			}
		}
	}

	protected void handleParalell(Reader reader, CsvLineHandler<T> handler, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(createCsvReader(reader, executor), handler, fileEntryHandler, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}	
	
	protected Reader createReader(InputStream in) {
		return new InputStreamReader(in, StandardCharsets.UTF_8);
	}

	protected void handle(CsvReader<T> reader, CsvLineHandler<T> handler, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
		do {
			T value = reader.next();
			if(value == null) {
				break;
			}
			handler.handleLine(value);
		} while(true);
		
		fileEntryHandler.endFileEntry(name, executor);
	}

	protected abstract CsvReader<T> createCsvReader(Reader reader, ThreadPoolExecutor executor) throws Exception;

}