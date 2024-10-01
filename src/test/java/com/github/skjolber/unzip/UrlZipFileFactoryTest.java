package com.github.skjolber.unzip;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UrlZipFileFactoryTest {

    @LocalServerPort
    private int randomServerPort;
	
	@Test
	public void testLocal() throws IOException {
		URL url = new URL("http://localhost:" + randomServerPort + "/rb_norway-aggregated-gtfs.zip");
		
		int count = Math.max(8, Runtime.getRuntime().availableProcessors());

		UrlZipFileFactory f = new UrlZipFileFactory(url, 1024*256, count);
		
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(1 * 1024 * 1024, new TestUnivocityCsvFileEntryHandler(new NoopUnivocityCsvLineHandlerFactory()));

		ZipFileEngine engine = new ZipFileEngine(handler, count);
		try {
			assertTrue(engine.handle(f));
		} finally {
			engine.close();
		}
	}
	
	@Test
	@Disabled
	public void testRemoteMultiThread() throws IOException {
		long time = System.currentTimeMillis();

		URL url = new URL("https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip");
		
		int count = Math.max(8, Runtime.getRuntime().availableProcessors());

		UrlZipFileFactory f = new UrlZipFileFactory(url, 4 * 1024*1024, count);
		
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(32 * 1024 * 1024, new TestUnivocityCsvFileEntryHandler(new NoopUnivocityCsvLineHandlerFactory()));

		ZipFileEngine engine = new ZipFileEngine(handler, count);
		try {
			assertTrue(engine.handle(f));
		} finally {
			engine.close();
		}
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for file size " + (f.size() / (1024*1024)) + " MB for " + count + " threads");
	}

	@Test
	@Disabled  
	public void testRemoteSingleThread() throws IOException {
		long time = System.currentTimeMillis();

		URL url = new URL("https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip");
		
		UrlZipFileFactory f = new UrlZipFileFactory(url, 2 * 1024*1024);
		
		ZipFileEngine engine = new ZipFileEngine(new TestUnivocityCsvFileEntryHandler(new NoopUnivocityCsvLineHandlerFactory()), 1);
		try {
			assertTrue(engine.handle(f));
		} finally {
			engine.close();
		}
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for file size " + (f.size() / (1024*1024)) + " MB for " + 1 + " threads");
	}
	

}
