package com.github.skjolber.unzip.csv;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.github.skjolber.unzip.ChunkSplitterFileEntryHandler;
import com.github.skjolber.unzip.FileEntryHandler;
import com.github.skjolber.unzip.FileZipFileFactory;
import com.github.skjolber.unzip.TestSesselTjonnaCsvFileEntryHandler;
import com.github.skjolber.unzip.TestSesselTjonnaCsvFileEntryHandler2;
import com.github.skjolber.unzip.ZipFileEngine;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SesselTjonnaZipFileTest {

	File file = new File("./src/test/resources/static/rb_norway-aggregated-gtfs.zip");
	
	@Test
	public void testSingleThread() throws Exception {
		FileEntryHandler handler = new TestSesselTjonnaCsvFileEntryHandler();
		
		System.gc();
		long time = System.currentTimeMillis();

		List<String> list = Arrays.asList("trips.txt");
		
		ZipFileEngine engine = new ZipFileEngine(handler, 1);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file), list));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for single thread");
	}

	
	@Test
	public void testMultiThread() throws Exception {
		FileEntryHandler handler = new TestSesselTjonnaCsvFileEntryHandler();
		
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
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for " + count + " cores");
	}
	
	@Test
	public void testMultiThreadWithChunks() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler());
		
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
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for " + count + " cores");
	}

	@Test
	public void testSingleThreadWithChunks() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler());
		
		System.gc();
		long time = System.currentTimeMillis();

		List<String> list = Arrays.asList("trips.txt");
		
		ZipFileEngine engine = new ZipFileEngine(handler, 1);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file), list));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for single core");
	}

	@Test
	public void testMultiThreadWithIntermediateProcessorAndChunks() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler2());
		
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
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for " + count + " cores");
	}

	@Test
	public void testSingleThreadWithIntermediateProcessorAndChunks() throws Exception {
		FileEntryHandler handler = new ChunkSplitterFileEntryHandler(2 * 1024 * 1024, new TestSesselTjonnaCsvFileEntryHandler2());
		
		System.gc();
		long time = System.currentTimeMillis();

		List<String> list = Arrays.asList("trips.txt");
		
		ZipFileEngine engine = new ZipFileEngine(handler, 1);
		try {
			assertTrue(engine.handle(new FileZipFileFactory(file), list));
		} finally {
			engine.close();
		}
		
		System.out.println("Used " + (System.currentTimeMillis() - time) + " millis for single core");
	}

}
