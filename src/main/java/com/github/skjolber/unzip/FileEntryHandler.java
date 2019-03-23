package com.github.skjolber.unzip;

import java.util.concurrent.ThreadPoolExecutor;

public interface FileEntryHandler {
	/**
	 * 
	 * Begin handling a file collection, i.e. zip file or directory
	 * 
	 * @param name name of zip file
	 */
	
	default void beginFileCollection(String name) {}
	
	/**
	 * Begin handling a file entry, i.e. a member of the file collection.
	 * 
	 * @param name name of file
	 */

	default void beginFileEntry(String name) {}
	
	/**
	 * Get handler for file entry
	 * 
	 * @param name name of file
	 * @param size size of file
	 * @param executor work delegation executor
	 * @return handler
	 * @throws Exception if a problem occored
	 */

	default FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor) throws Exception {
		return null;
	}

	/**
	 * End handling a file entry
	 * 
	 * @param name name of file
	 * @param executor work delegation executor
	 */
	
	default void endFileEntry(String name, ThreadPoolExecutor executor) {}

	/**
	 * End handling a file collection
	 * 
	 * @param name name of file
	 * @param executor work delegation executor
	 */

	default void endFileCollection(String name, ThreadPoolExecutor executor) {}
}
