package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.csv.CsvLineHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;

public class NoopSesselTjonnaCsvLineHandlerFactory implements CsvLineHandlerFactory {

	@Override
	public CsvLineHandler getHandler(String fileName, ThreadPoolExecutor executor) {
		System.out.println("Get handler for " + fileName + " in thread " + Thread.currentThread().getName());
		return new ObjectNoopCsvLineHandler();
	}

}
