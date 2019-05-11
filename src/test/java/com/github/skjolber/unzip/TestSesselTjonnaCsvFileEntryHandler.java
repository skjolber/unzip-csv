package com.github.skjolber.unzip;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvMapper;
import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.stcsv.builder.CsvBuilderException;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryChunkStreamHandler;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryStreamHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;
import com.github.skjolber.unzip.csv.Trip;

public class TestSesselTjonnaCsvFileEntryHandler implements ChunkedFileEntryHandler {

	protected NoopSesselTjonnaCsvLineHandlerFactory factory = new NoopSesselTjonnaCsvLineHandlerFactory();

	protected static CsvMapper<Trip> plain;

	static {
		try {
			plain = CsvMapper.builder(Trip.class)
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

		public TripCsvFileEntryStreamHandler(String name, CsvLineHandlerFactory csvLineHandlerFactory, long size, FileEntryHandler delegate, ThreadPoolExecutor executor) {
			super(name, csvLineHandlerFactory, size, delegate, executor);
		}

		@Override
		protected CsvReader<Trip> createCsvReader(Reader in, ThreadPoolExecutor executor) throws Exception {
			return plain.create(in);
		}
		
	}

	private static class TripCsvFileEntryChunkStreamHandler extends AbstractSesselTjonnaCsvFileEntryChunkStreamHandler<Trip> {
		
		public TripCsvFileEntryChunkStreamHandler(String name, Charset charset, FileChunkSplitter fileChunkSplitter, CsvLineHandlerFactory csvLineHandlerFactory) {
			super(name, charset, fileChunkSplitter, csvLineHandlerFactory);
		}

		@Override
		protected StaticCsvMapper<Trip> createStaticCsvMapper(String firstLine) throws Exception {
			return plain.buildStaticCsvMapper(firstLine);
		}
		
	}


	public TestSesselTjonnaCsvFileEntryHandler() throws CsvBuilderException {
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
			return new TripCsvFileEntryStreamHandler(name, factory, size, this, executor);
		}
		throw new RuntimeException();
	}

	@Override
	public FileEntryChunkStreamHandler getFileEntryChunkedStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		if(name.equals("trips.txt")) {
			return new TripCsvFileEntryChunkStreamHandler(name, StandardCharsets.UTF_8, new NewlineChunkSplitter(16 * 1024 * 1024), factory);
		}
		throw new RuntimeException();
	}


}
