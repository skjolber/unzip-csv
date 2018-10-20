package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.AbstractCsvReader;
import com.github.skjolber.stcsv.CsvMapper;
import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.CsvReaderConstructor;
import com.github.skjolber.unzip.FileEntryHandler;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractSesselTjonnaCsvFileEntryHandler extends AbstractCsvFileEntryHandler<Object> {

	protected Map<String, CsvReaderConstructor<Object>> headers = new ConcurrentHashMap<>();
	
	public AbstractSesselTjonnaCsvFileEntryHandler(CsvLineHandlerFactory<Object> csvLineHandlerFactory) {
		super(csvLineHandlerFactory);
	}
	
	public AbstractSesselTjonnaCsvFileEntryHandler() {
	}

	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		
		Reader reader = new InputStreamReader(in);
		
		CsvReaderConstructor<Object> mapper = headers.get(name);
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
			
			mapper = getMapper(builder.toString(), name);
		}

		CsvReader<Object> csvReader = mapper.newInstance(reader);
		
		// get the handler here so it is possible to maintain order within a handler factory (for the same file), if desired
		CsvLineHandler<Object> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			if(consume) {
				final FileEntryState fileEntryState = parts.get(name);
				fileEntryState.increment();
	
				handle(csvLineHandler, name, csvReader, executor);
	
				fileEntryState.decrement();
				
				notifyEndFileEntry(name, fileEntryState, executor);
			} else {
				execute(csvLineHandler, name, csvReader, executor);
			}
		} else {
			// ignore
		}
	}
	
	protected abstract CsvReaderConstructor<Object> getMapper(String header, String name);

	public void handle(CsvLineHandler<Object> csvLineHandler, String name, CsvReader<Object> reader, ThreadPoolExecutor executor) throws Exception {		
		do {
			Object line = reader.next();
			if(line == null) {
				break;
			}

			csvLineHandler.handleLine(line);
		} while(true);
	}
	
	public void execute(CsvLineHandler<Object> csvLineHandler, String name, CsvReader<Object> reader, ThreadPoolExecutor executor) throws Exception {
		final FileEntryState fileEntryState = parts.get(name);
		
		fileEntryState.increment();
		
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(csvLineHandler, name, reader, executor);
					
					fileEntryState.decrement();
					
					notifyEndFileEntry(name, fileEntryState, executor);
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
}
