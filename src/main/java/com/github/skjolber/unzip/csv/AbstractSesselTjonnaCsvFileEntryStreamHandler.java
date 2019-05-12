package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileEntryStreamHandler;

public abstract class AbstractSesselTjonnaCsvFileEntryStreamHandler<T> implements FileEntryStreamHandler {

	protected final String name;
	protected final CsvLineHandlerFactory csvLineHandlerFactory;
	protected final long parallelBufferSize;
	protected final FileEntryHandler fileEntryHandler;
	protected final ThreadPoolExecutor executor;

	public AbstractSesselTjonnaCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) {
		this(name, csvLineHandlerFactory, -1L, fileEntryHandler, executor);
	}

	public AbstractSesselTjonnaCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, long size, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) {
		super();
		this.name = name;
		this.csvLineHandlerFactory = csvLineHandlerFactory;
		this.parallelBufferSize = size;
		this.fileEntryHandler = fileEntryHandler;
		this.executor = executor;
	}

	@Override
	public void handle(InputStream in, boolean consume) throws Exception {
		CsvLineHandler<T> handler = csvLineHandlerFactory.getHandler(name, executor);
		if(handler != null) {
			Reader reader = createReader(in);
			if(parallelBufferSize != -1L) {
				// read the stream in this thread, queue the CSV parser itself in another job
				// note: the parallel buffer size must be as big as the whole file in order to prevent 
				// deadlocks
				Runnable r = handleParallel(reader, handler, fileEntryHandler, executor);
				r.run(); // consume input now
			} else {
				handle(createCsvReader(reader, executor), handler, fileEntryHandler, executor);
			}
		}
	}

	protected Runnable handleParallel(Reader reader, CsvLineHandler<T> handler, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
		ParallelReader parallelReader = new ParallelReader(reader, (int)parallelBufferSize);
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(createCsvReader(parallelReader, executor), handler, fileEntryHandler, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		return parallelReader.getRunnable();
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