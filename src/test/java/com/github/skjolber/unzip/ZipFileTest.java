package com.github.skjolber.unzip;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

public class ZipFileTest {

	public void testMultithread() throws IOException {
		FileEntryHandler handler = new NewLineSplitterEntryHandler(32 * 1024 * 1024, new TestCsvFileEntryHandler(new NoopCsvLineHandlerFactory()));
		
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
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for file size " + (file.length() / (1024*1024)) + " MB");
		
		
	}

}
