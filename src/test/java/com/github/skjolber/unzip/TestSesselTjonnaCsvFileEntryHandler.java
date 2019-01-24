package com.github.skjolber.unzip;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvMapper;
import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.stcsv.builder.CsvBuilderException;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;
import com.github.skjolber.unzip.csv.Trip;

public class TestSesselTjonnaCsvFileEntryHandler extends AbstractSesselTjonnaCsvFileEntryHandler {

	protected CsvMapper<Trip> plain;

	public TestSesselTjonnaCsvFileEntryHandler(CsvLineHandlerFactory csvLineHandlerFactory) throws CsvBuilderException {
		super(csvLineHandlerFactory);
		
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
	protected StaticCsvMapper getStaticCsvMapper(String name, byte[] byteArray) throws Exception {
		boolean carriageReturns = byteArray[byteArray.length - 2] == '\r';
		
		return plain.buildStaticCsvMapper(carriageReturns, new String(byteArray, StandardCharsets.UTF_8).trim());
	}

	@Override
	protected CsvReader getCsvReader(String name, InputStream in) throws Exception {
		if(name.equals("trips.txt")) {
			return plain.create(new InputStreamReader(in, StandardCharsets.UTF_8));
		}
		throw new RuntimeException();
	}

}
