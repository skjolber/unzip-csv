package com.github.skjolber.unzip;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.NewLineSplitterEntryHandler;
import com.github.skjolber.unzip.NoopCsvLineHandlerFactory;
import com.github.skjolber.unzip.TestCsvFileEntryHandler;
import com.github.skjolber.unzip.UrlZipFileFactory;
import com.github.skjolber.unzip.ZipFileEngine;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UrlZipFileFactoryTest {

    @LocalServerPort
    private int randomServerPort;
	
	@Test
	public void test() throws IOException {
		URL url = new URL("http://localhost:" + randomServerPort + "/rb_norway-aggregated-gtfs.zip");
		
		int count = Math.max(8, Runtime.getRuntime().availableProcessors());

		UrlZipFileFactory f = new UrlZipFileFactory(url, 1024*256, count);
		
		FileEntryHandler handler = new NewLineSplitterEntryHandler(1 * 1024 * 1024, new TestCsvFileEntryHandler(new NoopCsvLineHandlerFactory()));

		ZipFileEngine engine = new ZipFileEngine(handler, count);
		try {
			assertTrue(engine.handle(f));
		} finally {
			engine.close();
		}
	}
}
