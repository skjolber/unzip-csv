package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.unzip.FileEntryStreamHandler;

public abstract class AbstractSesselTjonnaCsvFileEntryStreamHandler<T> implements FileEntryStreamHandler {

	protected final String name;
	protected final CsvLineHandlerFactory csvLineHandlerFactory;
	
	public AbstractSesselTjonnaCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory) {
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