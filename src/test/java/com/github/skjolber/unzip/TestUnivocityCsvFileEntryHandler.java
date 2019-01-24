package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.csv.AbstractUnivocityCsvFileEntryHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;

public class TestUnivocityCsvFileEntryHandler extends AbstractUnivocityCsvFileEntryHandler {

	public TestUnivocityCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		super(csvLineHandlerFactory);
	}

	@Override
	public void beginFileEntry(String name) {
		System.out.println("Begin file entry for " + name);
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		System.out.println("End file entry for " + name);
	}

	@Override
	public void beginFileCollection(String name) {
		System.out.println("Begin zip file");
	}

	@Override
	public void endFileCollection(String name, ThreadPoolExecutor executor) {
		System.out.println("End zip file");
	}



}
