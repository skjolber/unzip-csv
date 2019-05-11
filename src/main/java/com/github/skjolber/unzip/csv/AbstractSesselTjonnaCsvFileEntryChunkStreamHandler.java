package com.github.skjolber.unzip.csv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.unzip.FileChunkSplitter;
import com.github.skjolber.unzip.FileEntryChunkStreamHandler;
import com.github.skjolber.unzip.FileEntryHandler;

public abstract class AbstractSesselTjonnaCsvFileEntryChunkStreamHandler<T> implements FileEntryChunkStreamHandler {

	protected final String name;
	protected StaticCsvMapper<T> mapper;
	protected Charset charset;
	protected FileChunkSplitter fileChunkSplitter;
	protected final CsvLineHandlerFactory csvLineHandlerFactory;

	public AbstractSesselTjonnaCsvFileEntryChunkStreamHandler(String name, Charset charset, FileChunkSplitter fileChunkSplitter, CsvLineHandlerFactory csvLineHandlerFactory) {
		super();
		this.name = name;
		this.charset = charset;
		this.fileChunkSplitter = fileChunkSplitter;
		this.csvLineHandlerFactory = csvLineHandlerFactory;
	}

	@Override
	public FileChunkSplitter getFileChunkSplitter() {
		return fileChunkSplitter;
	}

	@Override
	public void initialize(InputStream in, ThreadPoolExecutor executor) throws Exception {
		mapper = createStaticCsvMapper(getFirstLine(in));
	}

	@Override
	public void handleChunk(InputStream in, int chunkNumber, FileEntryHandler fileEntryHandler, ThreadPoolExecutor executor) throws Exception {
		CsvLineHandler<T> handler = csvLineHandlerFactory.getHandler(name, executor);
		if(handler != null) {
			handle(mapper.newInstance(new InputStreamReader(in, charset)), handler, fileEntryHandler, executor);
		} else {
			fileEntryHandler.endFileEntry(name, executor);
		}
	}

	public String getFirstLine(InputStream in) throws IOException {
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
		
		return new String(out.toByteArray(), charset);
	}
	
	protected abstract StaticCsvMapper<T> createStaticCsvMapper(String firstLine) throws Exception;

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

}
