package com.github.skjolber.unzip.csv;

import java.util.concurrent.ThreadPoolExecutor;

public interface CsvLineHandlerFactory {
	
	/**
	 * Return a handler for the file name. 
	 * 
	 * @param fileName file name
	 * @param executor for queuing additional work
	 * @return the line handler, null if none (ignored)
	 */

	CsvLineHandler getHandler(String fileName, ThreadPoolExecutor executor);

}
