package com.github.skjolber.unzip;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class ZipFileTest {

	@Test
	public void testMultiThread() throws IOException {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(32 * 1024 * 1024, new TestUnivocityCsvFileEntryHandler(new NoopUnivocityCsvLineHandlerFactory()));
		
		File file = new File("./src/test/resources/static/rb_norway-aggregated-gtfs.zip");
		
		System.gc();
		long time = System.currentTimeMillis();
		int count = Math.max(8, Runtime.getRuntime().availableProcessors());
		
		ZipFileEngine engine = new ZipFileEngine(handler, count);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file)));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for file size " + (file.length() / (1024*1024)) + " MB for " + count + " threads");
	}

	@Test
	public void testSingleThread() throws IOException {
		FileEntryHandler handler = new TestUnivocityCsvFileEntryHandler(new NoopUnivocityCsvLineHandlerFactory());
		
		File file = new File("./src/test/resources/static/rb_norway-aggregated-gtfs.zip");
		
		System.gc();
		long time = System.currentTimeMillis();
		
		ZipFileEngine engine = new ZipFileEngine(handler, 1);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file)));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for file size " + (file.length() / (1024*1024)) + " MB for " + 1 + " threads");
	}

}
