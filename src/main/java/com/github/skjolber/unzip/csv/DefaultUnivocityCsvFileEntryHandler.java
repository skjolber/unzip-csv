package com.github.skjolber.unzip.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.ChunkedFileEntryHandler;
import com.github.skjolber.unzip.FileChunkSplitter;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileEntryStreamHandler;
import com.github.skjolber.unzip.NewlineChunkSplitter;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * 
 * CSV {@linkplain FileEntryHandler}. Expects that the header can be extracted from the first input, if not already specified.
 * 
 */

public class DefaultUnivocityCsvFileEntryHandler implements ChunkedFileEntryHandler {

	protected class CsvFileEntryStreamHandler implements FileEntryStreamHandler {

		protected final String name;
		
		public CsvFileEntryStreamHandler(String name) {
			super();
			this.name = name;
		}

		@Override
		public void handle(InputStream in, boolean consume, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
			CsvParser reader = createCsvParser(in);
			
			String[] header = reader.parseNext();
			for(int i = 0; i < header.length; i++) {
				if(header[i] != null && header[i].trim().isEmpty()) {
					header[i] = null;
				}
			}
			
			CsvLineHandler<Map<String, String>> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
			if(csvLineHandler != null) {
				DefaultUnivocityCsvFileEntryHandler.this.handle(csvLineHandler, reader, header, executor);
			} else {
				// ignore
			}
			
			fileEntryHandler.endFileEntry(name, executor);
		}
		
	}

	protected class CsvFileEntryChunkStreamHandler implements FileEntryChunkStreamHandler {

		protected final String name;
		protected String[] headers;
		
		public CsvFileEntryChunkStreamHandler(String name) {
			super();
			this.name = name;
		}

		@Override
		public FileChunkSplitter getFileChunkSplitter() {
			return DefaultUnivocityCsvFileEntryHandler.this.getFileChunkSplitter(name);
		}

		@Override
		public void initialize(InputStream in, ThreadPoolExecutor executor) throws Exception {
			CsvParser reader = DefaultUnivocityCsvFileEntryHandler.this.createCsvParser(new ByteArrayInputStream(getFirstLine(in).toByteArray()));
			try {
				String[] header = reader.parseNext();
				for(int i = 0; i < header.length; i++) {
					if(header[i] != null && header[i].trim().isEmpty()) {
						header[i] = null;
					}
				}
				this.headers = header;
			} finally {
				reader.stopParsing();
			}
		}

		@Override
		public void handleChunk(InputStream in, int chunkNumber, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
			CsvLineHandler<Map<String, String>> csvLineHandler = csvLineHandlerFactory.getHandler(name, executor);
			if(csvLineHandler != null) {
				CsvParser reader = createCsvParser(in);

				handle(csvLineHandler, reader, headers, executor);
			}
			fileEntryHandler.endFileEntry(name, executor);
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
				if(read == '\n') {
					break;
				}
				out.write(read);
			} while(true);
			
			return out;
		}
		
	}

	protected CsvLineHandlerFactory csvLineHandlerFactory;
	protected int chunkLength;

	public DefaultUnivocityCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory, int chunkLength) {
		this.csvLineHandlerFactory = csvLineHandlerFactory;
		this.chunkLength = chunkLength;
	}
	
	public DefaultUnivocityCsvFileEntryHandler() {
	}

	@Override
	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return new CsvFileEntryStreamHandler(name);
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return new CsvFileEntryChunkStreamHandler(name);
	}

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
	
	public void handle(CsvLineHandler<Map<String, String>> csvLineHandler, CsvParser reader, String[] names, ThreadPoolExecutor executor) throws IOException {		
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
	
	protected FileChunkSplitter getFileChunkSplitter(String name) {
		return new NewlineChunkSplitter(chunkLength);
	}

}
