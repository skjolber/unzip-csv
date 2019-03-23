package com.github.skjolber.unzip.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Handler cache which limits creating new handlers to one per thread.
 *
 */

public class PerThreadCsvFileEntryHandlerFactory implements CsvLineHandlerFactory {

	protected Map<Thread, Map<String, CsvLineHandler<?>>> handlers = new ConcurrentHashMap<>();

	protected CsvLineHandlerFactory factory;
	
	public PerThreadCsvFileEntryHandlerFactory(CsvLineHandlerFactory factory) {
		this.factory = factory;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <L> CsvLineHandler<L> getHandler(String fileName, ThreadPoolExecutor executor) {

		Map<String, CsvLineHandler<?>> map = handlers.get(Thread.currentThread());
		if(map == null) {
			map = new HashMap<>();
			handlers.put(Thread.currentThread(), map);
		}

		CsvLineHandler<L> csvLineHandler = (CsvLineHandler<L>) map.get(fileName);
		if(csvLineHandler == null) {
			csvLineHandler = factory.getHandler(fileName, executor);
			map.put(fileName, csvLineHandler);
		}
		
		return csvLineHandler;
	}

	public Set<String> getFileNames() {
		Set<String> result = new HashSet<>();
		
		for (Entry<Thread, Map<String, CsvLineHandler<?>>> threadEntry : handlers.entrySet()) {
			for (Entry<String, CsvLineHandler<?>> fileEntity: threadEntry.getValue().entrySet()) {
				result.add(fileEntity.getKey());
			}
		}
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public <L> List<CsvLineHandler<L>> getHandlers(String fileName) {
		List<CsvLineHandler<L>> list = new ArrayList<>();
		
		for (Entry<Thread, Map<String, CsvLineHandler<?>>> threadEntry : handlers.entrySet()) {
			for (Entry<String, CsvLineHandler<?>> fileEntity: threadEntry.getValue().entrySet()) {
				if(fileEntity.getKey().equals(fileName)) {
					list.add((CsvLineHandler<L>) fileEntity.getValue());
				}
			}
		}
		
		return list;
	}

}
