package com.github.skjolber.unzip.csv;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.github.skjolber.unzip.ChunkSplitterFileEntryHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileZipFileFactory;
import com.github.skjolber.unzip.NoopSesselTjonnaCsvLineHandlerFactory;
import com.github.skjolber.unzip.TestSesselTjonnaCsvFileEntryHandler;
import com.github.skjolber.unzip.TestSesselTjonnaCsvFileEntryHandler2;
import com.github.skjolber.unzip.ZipFileEngine;

public class SesselTjonnaZipFileTest2 {

	@Test
	public void testMultiThreadWithIntermediateProcessor() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler2(new NoopSesselTjonnaCsvLineHandlerFactory()));
		
		File file = new File("./src/test/resources/static/rb_norway-aggregated-gtfs.zip");
		
		System.gc();
		long time = System.currentTimeMillis();
		int count = Math.max(8, Runtime.getRuntime().availableProcessors());

		List<String> list = Arrays.asList("trips.txt");

		ZipFileEngine engine = new ZipFileEngine(handler, count);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file), list));
		} finally {
			engine.close();
		}
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis");
	}

	@Test
	public void testSingleThreadWithIntermediateProcessor() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler2	(new NoopSesselTjonnaCsvLineHandlerFactory()));
		
		File file = new File("./src/test/resources/static/rb_norway-aggregated-gtfs.zip");
		
		System.gc();
		long time = System.currentTimeMillis();

		List<String> list = Arrays.asList("trips.txt");
		
		ZipFileEngine engine = new ZipFileEngine(handler, 1);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file), list));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis");

	}

}
