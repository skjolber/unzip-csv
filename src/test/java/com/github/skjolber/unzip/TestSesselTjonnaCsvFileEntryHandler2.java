package com.github.skjolber.unzip;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.stcsv.CsvMapper2;
import com.github.skjolber.stcsv.CsvReader;
import com.github.skjolber.stcsv.StaticCsvMapper;
import com.github.skjolber.stcsv.StaticCsvMapper2;
import com.github.skjolber.stcsv.builder.CsvBuilderException;
import com.github.skjolber.unzip.csv.AbstractSesselTjonnaCsvFileEntryHandler;
import com.github.skjolber.unzip.csv.CsvLineHandlerFactory;
import com.github.skjolber.unzip.csv.Trip;

public class TestSesselTjonnaCsvFileEntryHandler2 extends AbstractSesselTjonnaCsvFileEntryHandler {

	private CsvMapper2<Trip, Cache> plain;

	private List<Cache> caches = Collections.synchronizedList(new ArrayList<>());
	
	public static class Cache {
		private Set<String> routes = new HashSet<>();
		
		public void add(String str) {
			routes.add(str);
		}
		
		public Set<String> getRoutes() {
			return routes;
		}
	}
	
	public TestSesselTjonnaCsvFileEntryHandler2(CsvLineHandlerFactory csvLineHandlerFactory) throws CsvBuilderException {
		super(csvLineHandlerFactory);
		
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
	protected StaticCsvMapper getStaticCsvMapper(String name, String firstLine) throws Exception {
		if(name.equals("trips.txt")) {
			StaticCsvMapper2<Trip, Cache> buildStaticCsvMapper = plain.buildStaticCsvMapper(firstLine);
			
			return new StaticCsvMapperAdapter(buildStaticCsvMapper) {
	
				@Override
				protected Object newIntermediateProcessor() {
					System.out.println("Create delegate cache");
					Cache cache = new Cache();
					caches.add(cache);
	
					return cache;
				}
				
			};
		} else {
			throw new RuntimeException();
		}
	}

	@Override
	protected CsvReader getCsvReader(String name, InputStream in) throws Exception {
		if(name.equals("trips.txt")) {
			System.out.println("Create cache");
			Cache cache = new Cache();
			caches.add(cache);
			return plain.create(new InputStreamReader(in, StandardCharsets.UTF_8), cache);
		}
		throw new RuntimeException();
	}

}
