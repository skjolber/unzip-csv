package com.github.skjolber.unzip.csv;

public interface CsvLineHandler<T> {

	/**
	 * Handle line in key-value form.
	 * 
	 * @param value line value
	 */
	void handleLine(T value);
	
}
