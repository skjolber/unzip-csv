package com.github.skjolber.unzip;

public interface ChunkedFileEntryHandler extends FileEntryHandler {

	FileChunkSplitter splitFileEntry(final String name, long size);
	
}
