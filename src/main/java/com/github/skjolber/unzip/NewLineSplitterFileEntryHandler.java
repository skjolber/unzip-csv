package com.github.skjolber.unzip;

public interface NewLineSplitterFileEntryHandler extends FileEntryHandler {

	boolean shouldSplit(final String name, long size);
	
}
