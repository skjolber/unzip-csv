package com.github.skjolber.unzip.csv;

import java.util.Map;

public interface CsvLineHandler {

	/**
	 * Handle line in key-value form
	 * @param fields key-value map, which will be reset by the caller before reading the next line
	 */
	void handleLine(Map<String, String> fields);
	
}
