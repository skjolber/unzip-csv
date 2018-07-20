package com.github.skjolber.unzip.csv;

import java.util.concurrent.ThreadPoolExecutor;

public interface CsvLineHandlerFactory {

	CsvLineHandler getHandler(String fileName, ThreadPoolExecutor executor);

}
