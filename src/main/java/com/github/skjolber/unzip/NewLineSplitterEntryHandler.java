package com.github.skjolber.unzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Read and split inputs into larger parts based on newline. 
 * 
 */

public class NewLineSplitterEntryHandler implements FileEntryHandler {

	protected final int chuckLength; // effective length depends on line lengths
	protected final FileEntryHandler delegate;
	
	/**
	 * Constructor.
	 * 
	 * @param chuckLength number of bytes per segment
	 * @param delegate delegate for forwarding (whole or partial) streams. 
	 */
	
	public NewLineSplitterEntryHandler(int chuckLength, FileEntryHandler delegate) {
		super();
		this.chuckLength = chuckLength;
		this.delegate = delegate;
	}

	public void handle(final String name, long size, InputStream in, final ThreadPoolExecutor executor, boolean consume) throws Exception {
		if(size > chuckLength) {
			byte[] buffer = new byte[Math.min(8192 * 16, chuckLength)];

			ByteArrayOutputStream bout = new ByteArrayOutputStream(chuckLength);
			
			while(true) {
				
				int remaining = chuckLength;
				
				int read;
				do {
					read = in.read(buffer, 0, Math.min(buffer.length, remaining));

					if(read == -1) {
						break;
					}
					
					remaining -= read;

					bout.write(buffer, 0, read);
				} while(remaining > 0);
				
				final byte[] byteArray = bout.toByteArray();
				
				if(read == -1) {
					// end of file
					delegate.handle(name, byteArray.length, new ByteArrayInputStream(byteArray), executor, false);
					
					break;
				} else {
					int index = byteArray.length - 1;
					
					// seek backward for a newline
					while(index >= 0) {
						if(byteArray[index] == '\n') {
							break;
						}
						index--;
					}
					if(index == -1) {
						throw new IllegalArgumentException("No newline found in chunk size " + byteArray.length);
					}
					delegate.handle(name, size, new ByteArrayInputStream(byteArray, 0, index - 1), executor, false);

					// reuse buffer
					bout.reset();
					// write tail
					if(index + 1 < byteArray.length) {
						bout.write(byteArray, index + 1, byteArray.length - index - 1);
					}
				}
			}
		} else {
			delegate.handle(name, size, in, executor, consume);
		}
	}

	@Override
	public void beginFileEntry(String name) {
		delegate.beginFileEntry(name);
	}

	@Override
	public void endFileEntry(String name, ThreadPoolExecutor executor) {
		delegate.endFileEntry(name, executor);
	}

	@Override
	public void beginFileCollection(String name) {
		delegate.beginFileCollection(name);
	}

	@Override
	public void endFileCollection(String name, ThreadPoolExecutor executor) {
		delegate.endFileCollection(name, executor);
	}
	
}
