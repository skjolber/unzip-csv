package com.github.skjolber.unzip;

import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.builder.CsvBuilderException;
import com.github.skjolber.stcsv.databinder.CsvMapper2;
import com.github.skjolber.stcsv.databinder.StaticCsvMapper;
import com.github.skjolber.stcsv.databinder.StaticCsvMapper2;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryChunkStreamHandler;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryStreamHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;
import com.github.skjolber.unzip.csv.Trip;

public class TestSesselTjonnaCsvFileEntryHandler2 implements ChunkedFileEntryHandler {

	public static class Cache {
		private Set<String> routes = new HashSet<>();
		
		public void add(String str) {
			routes.add(str);
		}
		
		public Set<String> getRoutes() {
			return routes;
		}
	}
	
	protected static abstract class StaticCsvMapperAdapter<T, D> implements StaticCsvMapper<T> {

		protected StaticCsvMapper2<T, D> staticCsvMapper2;
		
		public StaticCsvMapperAdapter(StaticCsvMapper2<T, D> staticCsvMapper2) {
			this.staticCsvMapper2 = staticCsvMapper2;
		}

		@Override
		public CsvReader<T> newInstance(Reader reader) {
			return staticCsvMapper2.newInstance(reader, newIntermediateProcessor());
		}

		@Override
		public CsvReader<T> newInstance(Reader reader, char[] current, int offset, int length) {
			return staticCsvMapper2.newInstance(reader, current, offset, length, newIntermediateProcessor());
		}

		protected abstract D newIntermediateProcessor();
	}
	

	protected NoopSesselTjonnaCsvLineHandlerFactory factory = new NoopSesselTjonnaCsvLineHandlerFactory();

	protected static CsvMapper2<Trip, Cache> plain;

	private List<Cache> caches = Collections.synchronizedList(new ArrayList<>());

	protected Cache getCache() {
		Cache cache = new Cache();
		caches.add(cache);
		return cache;
	}
	
	static {
		try {
			plain = CsvMapper2.builder(Trip.class, Cache.class)
					.stringField("route_id")
						.setter(Trip::setRouteId)
						.quoted()
						.optional()
					.stringField("service_id")
						.setter(Trip::setServiceId)
						.quoted()
						.required()
					.stringField("trip_id")
						.setter(Trip::setTripId)
						.quoted()
						.required()
					.stringField("trip_headsign")
						.setter(Trip::setTripHeadsign)
						.quoted()
						.optional()
					.integerField("direction_id")
						.setter(Trip::setDirectionId)
						.quoted()
						.optional()
					.stringField("shape_id")
						.setter(Trip::setShapeId)
						.quoted()
						.optional()
					.integerField("wheelchair_accessible")
						.setter(Trip::setWheelchairAccessible)
						.quoted()
						.optional()
					.build();
		} catch (CsvBuilderException e) {
			throw new RuntimeException();
		}
	}

	private static class TripCsvFileEntryStreamHandler extends AbstractSesselTjonnaCsvFileEntryStreamHandler<Trip> {

		private TestSesselTjonnaCsvFileEntryHandler2 cacheFactory;
		
		public TripCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, TestSesselTjonnaCsvFileEntryHandler2 cacheFactory, long size, FileEntryHandler delegate, ThreadPoolExecutor executor) {
			super(name, csvLineHandlerFactory, size, delegate, executor);
			
			this.cacheFactory = cacheFactory;
		}

		@Override
		protected CsvReader<Trip> createCsvReader(Reader reader, ThreadPoolExecutor executor) throws Exception {
			return plain.create(reader, cacheFactory.getCache());
		}
		
	}

	private static class TripCsvFileEntryChunkStreamHandler extends AbstractSesselTjonnaCsvFileEntryChunkStreamHandler<Trip> {
		
		private TestSesselTjonnaCsvFileEntryHandler2 cacheFactory;

		public TripCsvFileEntryChunkStreamHandler(String name, Charset charset, FileChunkSplitter fileChunkSplitter, CsvLineHandlerFactory csvLineHandlerFactory, TestSesselTjonnaCsvFileEntryHandler2 cacheFactory) {
			super(name, charset, fileChunkSplitter, csvLineHandlerFactory);
			
			this.cacheFactory = cacheFactory;
		}

		@Override
		protected StaticCsvMapper<Trip> createStaticCsvMapper(String firstLine) throws Exception {
			StaticCsvMapper2<Trip, Cache> buildStaticCsvMapper = plain.buildStaticCsvMapper(firstLine);
			
			return new StaticCsvMapperAdapter<Trip, Cache>(buildStaticCsvMapper) {

				@Override
				protected Cache newIntermediateProcessor() {
					return cacheFactory.getCache();
				}
			};
		}
		
	}


	public TestSesselTjonnaCsvFileEntryHandler2() throws CsvBuilderException {
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
	
	@Override
	public FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		if(name.equals("trips.txt")) {
			return new TripCsvFileEntryStreamHandler(name, factory, this, size, this, executor);
		}
		throw new RuntimeException();
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		if(name.equals("trips.txt")) {
			return new TripCsvFileEntryChunkStreamHandler(name, StandardCharsets.UTF_8, new NewlineChunkSplitter(16 * 1024 * 1024), factory, this);
		}
		throw new RuntimeException();
	}
}
