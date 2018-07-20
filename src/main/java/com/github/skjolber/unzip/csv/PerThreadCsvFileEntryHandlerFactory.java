package com.github.skjolber.unzip.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.skjolber.unzip.csv.CsvLineHandler;

public class PerThreadCsvFileEntryHandlerFactory implements CsvLineHandlerFactory {

	protected Map<Thread, Map<String, CsvLineHandler>> handlers = new ConcurrentHashMap<>();

	protected CsvLineHandlerFactory factory;
	
	public PerThreadCsvFileEntryHandlerFactory(CsvLineHandlerFactory factory) {
		this.factory = factory;
	}

	@Override
	public CsvLineHandler getHandler(String fileName, ThreadPoolExecutor executor) {

		Map<String, CsvLineHandler> map = handlers.get(Thread.currentThread());
		if(map == null) {
			map = new HashMap<>();
			handlers.put(Thread.currentThread(), map);
		}

		CsvLineHandler csvLineHandler = map.get(fileName);
		if(csvLineHandler == null) {
			csvLineHandler = factory.getHandler(fileName, executor);
			map.put(fileName, csvLineHandler);
		}
		
		return csvLineHandler;
	}

	public Map<String, List<CsvLineHandler>> getHandlers() {
		Map<String, List<CsvLineHandler>> result = new HashMap<>();
		
		for (Entry<Thread, Map<String, CsvLineHandler>> threadEntry : handlers.entrySet()) {
			for (Entry<String, CsvLineHandler> fileEntity: threadEntry.getValue().entrySet()) {
				List<CsvLineHandler> list = result.get(fileEntity.getKey());
				if(list == null) {
					list = new ArrayList<>();
					result.put(fileEntity.getKey(), list);
				}
				list.add(fileEntity.getValue());
			}
		}
		
		return result;
	}
}
