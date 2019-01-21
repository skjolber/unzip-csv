package com.github.skjolber.unzip.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.AbstractCsvMapper;
import com.github.skjolber.stcsv.AbstractCsvReader;
import com.github.skjolber.stcsv.CsvMapper2;
import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.unzip.FileEntryHandler;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractSesselTjonnaCsvFileEntryHandler extends AbstractCsvFileEntryHandler {
	
	protected Map<String, H> constructors = new ConcurrentHashMap<>();
	
	public AbstractSesselTjonnaCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}
	
	public AbstractSesselTjonnaCsvFileEntryHandler() {
	}

	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		// loose coupling between factory and parser
		handleImpl(name, size, in, executor, consume);
	}
	
	protected <T> void handleImpl(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {

		Reader reader = new InputStreamReader(in);

		H mapper = constructors.get(name);
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
			
			//boolean carriageReturn = builder.charAt(builder.length() - 1) == '\r';
			
			mapper = getCsvMapper(name, builder.toString());
		}

		AbstractCsvReader<? extends T> csvReader = mapper.newInstance(reader);
		
		// get the handler here so it is possible to maintain order within a handler factory (for the same file), if desired
		CsvLineHandler<T> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			if(consume) {
				final FileEntryChunkState fileEntryState = parts.get(name);
				fileEntryState.increment();
	
				handle(csvLineHandler, name, csvReader, executor);
	
				fileEntryState.decrement();
				
				notifyEndFileEntry(name, fileEntryState, executor);
				
				notifyEndHandler(csvLineHandler, name, executor);
			} else {
				execute(csvLineHandler, name, csvReader, executor);
			}
		}
	}
	
	protected abstract H getCsvMapper(String name, String string);

	protected abstract <T> CsvReaderConstructor<T> getReaderConstructor(String header, String name);

	protected abstract void handle(CsvReader<? extends T> reader, ThreadPoolExecutor executor) throws Exception;
	
	
	public <T> void execute(CsvLineHandler<T> csvLineHandler, String name, CsvReader<? extends T> reader, ThreadPoolExecutor executor) throws Exception {
		final FileEntryChunkState fileEntryState = parts.get(name);
		
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
		parts.put(name, new FileEntryChunkState());
	}
	
	protected abstract void notifyEndHandler(CsvLineHandler<?> csvLineHandler, String name, ThreadPoolExecutor executor);

}










