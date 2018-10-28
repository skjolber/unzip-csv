package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.CsvReaderConstructor;
import com.github.skjolber.unzip.FileEntryHandler;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractSesselTjonnaCsvFileEntryHandler<T> extends AbstractCsvFileEntryHandler<T> {

	protected Map<String, CsvReaderConstructor<T>> constructors = new ConcurrentHashMap<>();
	protected CsvLineHandlerFactory csvLineHandlerFactory;
	
	public AbstractSesselTjonnaCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}
	
	public AbstractSesselTjonnaCsvFileEntryHandler() {
	}

	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		
		Reader reader = new InputStreamReader(in);
		
		CsvReaderConstructor<T> mapper = constructors.get(name);
		if(mapper == null) {
			StringBuilder builder = new StringBuilder();
			
			int read;
			do {
				
				read = reader.read();
				if(read == -1) {
					return;
				}
				if(read == '\n') {
					break;
				}
				builder.append((char)read);
			} while(true);
			
			mapper = getReaderConstructor(builder.toString(), name);
		}

		CsvReader<T> csvReader = mapper.newInstance(reader);
		
		// get the handler here so it is possible to maintain order within a handler factory (for the same file), if desired
		CsvLineHandler<T> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			if(consume) {
				final FileEntryState fileEntryState = parts.get(name);
				fileEntryState.increment();
	
				handle(csvLineHandler, name, csvReader, executor);
	
				fileEntryState.decrement();
				
				notifyEndFileEntry(name, fileEntryState, executor);
				
				notifyEndHandler(csvLineHandler, name, executor);
			} else {
				execute(csvLineHandler, name, csvReader, executor);
			}
		} else {
			// ignore
		}
	}
	
	protected abstract CsvReaderConstructor<T> getReaderConstructor(String header, String name);

	public void handle(CsvLineHandler<T> csvLineHandler, String name, CsvReader<T> reader, ThreadPoolExecutor executor) throws Exception {		
		do {
			T line = reader.next();
			if(line == null) {
				break;
			}

			csvLineHandler.handleLine(line);
		} while(true);
	}
	
	public void execute(CsvLineHandler<T> csvLineHandler, String name, CsvReader<T> reader, ThreadPoolExecutor executor) throws Exception {
		final FileEntryState fileEntryState = parts.get(name);
		
		fileEntryState.increment();
		
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(csvLineHandler, name, reader, executor);
					
					fileEntryState.decrement();
					
					notifyEndFileEntry(name, fileEntryState, executor);
					
					notifyEndHandler(csvLineHandler, name, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	@Override
	public void beginFileEntry(String name) {
		parts.put(name, new FileEntryState());
	}
	
	protected abstract void notifyEndHandler(CsvLineHandler<T> csvLineHandler, String name, ThreadPoolExecutor executor);

}










