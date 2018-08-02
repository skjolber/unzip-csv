package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.csv.AbstractCsvFileEntryHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;

public class TestCsvFileEntryHandler extends AbstractCsvFileEntryHandler {

	public TestCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) {
		super(csvLineHandlerFactory);
	}

	@Override
	public void beginFileEntry(String name) {
		System.out.println("Begin file entry for " + name);
		super.beginFileEntry(name);
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		System.out.println("End file entry for " + name);
		super.endFileEntry(name, executor);
	}

	@Override
	protected void endFileEntryProcessing(String name, ThreadPoolExecutor executor) {
		System.out.println("End handle async for " + name);
	}

	@Override
	public void beginFileCollection(String name) {
		System.out.println("Begin zip file");
	}

	@Override
	public void endFileCollection(String name, ThreadPoolExecutor executor) {
		System.out.println("End zip file");
	}

	@Override
	public boolean splitFileEntry(String name, long size) {
		return true;
	}

}
