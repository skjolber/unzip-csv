package com.github.skjolber.unzip;

public interface NewLineSplitterFileEntryHandler extends FileEntryHandler {

	boolean splitFileEntry(final String name, long size);
	
}
