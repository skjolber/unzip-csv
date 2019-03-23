package com.github.skjolber.unzip;

import java.util.Map;

import com.github.skjolber.unzip.csv.CsvLineHandler;

public class MapNoopCsvLineHandler implements CsvLineHandler<Map<String, String>> {

	@Override
	public void handleLine(Map<String, String> fields) {
		if(fields.isEmpty()) {
			throw new RuntimeException();
		}
	}

}
