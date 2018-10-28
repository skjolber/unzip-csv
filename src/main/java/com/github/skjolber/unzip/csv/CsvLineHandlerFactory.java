package com.github.skjolber.unzip.csv;

import java.util.concurrent.ThreadPoolExecutor;

public interface CsvLineHandlerFactory {
	
	/**
	 * Return a handler for the file name. 
	 * 
	 * @param fileName file name
	 * @param executor for queuing additional work
	 * @param <T> the expected type of object returned per line
	 * @return the line handler, null if none (ignored)
	 */

	<T> CsvLineHandler<T> getHandler(String fileName, ThreadPoolExecutor executor);

}
