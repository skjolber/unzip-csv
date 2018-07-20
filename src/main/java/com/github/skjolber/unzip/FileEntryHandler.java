package com.github.skjolber.unzip;

import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

public interface FileEntryHandler {
	/**
	 * 
	 * Begin handling a file collection, i.e. zip file or directory
	 * 
	 * @param name name of zip file
	 */
	
	void beginFileCollection(String name);
	
	/**
	 * Begin handling a file entry, i.e. a member of the file collection.
	 * 
	 * @param name name of file
	 */

	void beginFileEntry(String name);

	/**
	 * Handle a file entry
	 * 
	 * @param name name of file
	 * @param size file size
	 * @param in file binary stream
	 * @param executor work delegation executor
	 * @param consume if true, the stream must be consumed in the current thread (not delegated to executor)
	 * @throws Exception if a problem occurs
	 */
	void handle(String name, long size, InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception;

	/**
	 * End handling a file entry
	 * 
	 * @param name name of file
	 * @param executor work delegation executor
	 */
	
	void endFileEntry(String name, ThreadPoolExecutor executor);

	/**
	 * End handling a file collection
	 * 
	 * @param name name of file
	 * @param executor work delegation executor
	 */

	void endFileCollection(String name, ThreadPoolExecutor executor);
}
