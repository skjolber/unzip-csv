package com.github.skjolber.unzip.csv;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.skjolber.unzip.FileEntryHandler;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractCsvFileEntryHandler implements FileEntryHandler {

	protected static class FileEntryState {
		private AtomicInteger count = new AtomicInteger(0);
		protected volatile boolean ended = false;
		protected volatile boolean notified = false;
		
		public int increment() {
			return count.incrementAndGet();
		}

		public int decrement() {
			return count.decrementAndGet();
		}
		
		public int get() {
			return count.get();
		}

		public void ended() {
			ended = true;
		}
		
		public boolean isEnded() {
			return ended;
		}
		
		public void notified() {
			notified = true;
		}
		
		public boolean isNotified() {
			return notified;
		}
		
	}

	protected Map<String, String[]> headers = new ConcurrentHashMap<>();
	
	protected Map<String, FileEntryState> parts = Collections.synchronizedMap(new HashMap<>());
	
	protected CsvLineHandlerFactory csvLineHandlerFactory;
	
	public AbstractCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}
	
	public AbstractCsvFileEntryHandler() {
	}

	public void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		CsvParser reader = createCsvParser(in);
		
		String[] header = this.headers.get(name);
		if(header == null) {
			header = reader.parseNext();
			for(int i = 0; i < header.length; i++) {
				if(header[i] != null && header[i].trim().isEmpty()) {
					header[i] = null;
				}
			}
			this.headers.put(name, header);
		}
		
		if(consume) {
			final FileEntryState fileEntryState = parts.get(name);
			fileEntryState.increment();

			handle(name, reader, header, executor);

			fileEntryState.decrement();
			
			notifyEndFileEntry(name, fileEntryState, executor);
		} else {
			execute(name, reader, header, executor);
		}
	}

	/**
	 * Override this method to customize parser
	 * 
	 * @return parser settings
	 */

	protected CsvParser createCsvParser(InputStream in) {
		CsvParserSettings settings = createCsvParserSettings();
		
		CsvParser parser = new CsvParser(settings);
		
		parser.beginParsing(in, StandardCharsets.UTF_8);
		
		return parser;
	}
	
	/**
	 * Override this method to customize parser settings
	 * 
	 * @return parser settings
	 */

	protected CsvParserSettings createCsvParserSettings() {
		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setLineSeparator("\n");
		return settings;
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		FileEntryState fileEntryState = parts.get(name);
		fileEntryState.ended();
		notifyEndFileEntry(name, fileEntryState, executor);
	}

	protected void notifyEndFileEntry(String name, FileEntryState fileEntryState, ThreadPoolExecutor executor) {
		synchronized (fileEntryState) {
			if(fileEntryState.get() == 0 && fileEntryState.isEnded() && !fileEntryState.isNotified()) {
				fileEntryState.notified();
				
				endFileEntryProcessing(name, executor);
				
				parts.remove(name);
			}
			
		}
	}
	/**
	 * Notify when processing is performed
	 * 
	 * @param name name of file processed
	 * @param executor 
	 */

	protected abstract void endFileEntryProcessing(String name, ThreadPoolExecutor executor);

	public void execute(String name, CsvParser reader, String[] names, ThreadPoolExecutor executor) throws Exception {
		final FileEntryState fileEntryState = parts.get(name);
		
		fileEntryState.increment();
		
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(name, reader, names, executor);
					
					fileEntryState.decrement();
					
					notifyEndFileEntry(name, fileEntryState, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}
	
	public void handle(String name, CsvParser reader, String[] names, ThreadPoolExecutor executor) throws IOException {
		CsvLineHandler csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		
		Map<String, String> fields = new HashMap<>(256);
		
		try {
			do {
				String[] line = reader.parseNext();
				if(line == null) {
					break;
				}

				for (int i = 0; i < line.length; i++) {
					String string = line[i];
					if(string != null && !string.isEmpty()) {
						fields.put(names[i], string);
					}
				}
				if(!fields.isEmpty()) {
					csvLineHandler.handleLine(fields);
					
					fields.clear();
				}
			} while(true);
		} finally {
			reader.stopParsing();
		}		
	}

	public String[] get(String key) {
		return headers.get(key);
	}

	public String[] put(String key, String[] value) {
		return headers.put(key, value);
	}

	public String[] remove(Object key) {
		return headers.remove(key);
	}

	@Override
	public void beginFileEntry(String name) {
		parts.put(name, new FileEntryState());
	}
}
