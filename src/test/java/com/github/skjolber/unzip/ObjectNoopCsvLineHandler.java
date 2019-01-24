package com.github.skjolber.unzip;

import com.github.skjolber.unzip.csv.CsvLineHandler;

public class ObjectNoopCsvLineHandler<T> implements CsvLineHandler<T> {

	@Override
	public void handleLine(T line) {
		if(line == null) {
			throw new RuntimeException();
		}
	}

}
