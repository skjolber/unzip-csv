package com.github.skjolber.unzip.csv;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.FileEntryHandler;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public abstract class AbstractUnivocityCsvFileEntryHandler extends AbstractCsvFileEntryHandler<Map<String, String>> {

	protected Map<String, String[]> headers = new ConcurrentHashMap<>();
	protected CsvLineHandlerFactory csvLineHandlerFactory;

	public AbstractUnivocityCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}
	
	public AbstractUnivocityCsvFileEntryHandler() {
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
		
		// get the handler here so it is possible to maintain order within a handler factory (for the same file), if desired
		CsvLineHandler<Map<String, String>> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
		if(csvLineHandler != null) {
			if(consume) {
				final FileEntryState fileEntryState = parts.get(name);
				fileEntryState.increment();
	
				handle(csvLineHandler, name, reader, header, executor);
	
				fileEntryState.decrement();
				
				notifyEndFileEntry(name, fileEntryState, executor);
				
				notifyEndHandler(csvLineHandler, name, executor);
				
			} else {
				execute(csvLineHandler, name, reader, header, executor);
			}
		} else {
			// ignore
		}
	}

	protected abstract void notifyEndHandler(CsvLineHandler<Map<String, String>> csvLineHandler, String name, ThreadPoolExecutor executor);

	/**
	 * Create parser
	 * 
	 * @param in stream to parse
	 * @return parser
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
	
	public void handle(CsvLineHandler<Map<String, String>> csvLineHandler, String name, CsvParser reader, String[] names, ThreadPoolExecutor executor) throws IOException {		
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
	
	
	public void execute(CsvLineHandler<Map<String, String>> csvLineHandler, String name, CsvParser reader, String[] names, ThreadPoolExecutor executor) {
		final FileEntryState fileEntryState = parts.get(name);
		
		fileEntryState.increment();
		
		executor.execute(new Runnable() {
			public void run() {
				try {
					handle(csvLineHandler, name, reader, names, executor);
					
					fileEntryState.decrement();
					
					notifyEndFileEntry(name, fileEntryState, executor);
					
					notifyEndHandler(csvLineHandler, name, executor);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
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
