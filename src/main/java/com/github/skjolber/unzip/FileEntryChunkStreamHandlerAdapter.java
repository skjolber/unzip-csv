package com.github.skjolber.unzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

public class FileEntryChunkStreamHandlerAdapter implements FileEntryStreamHandler {

	protected final int chuckLength;
	protected final FileEntryChunkState fileEntryState;
	protected final FileEntryChunkStreamHandler delegate;
	
	public FileEntryChunkStreamHandlerAdapter(int chuckLength, FileEntryChunkState fileEntryState, FileEntryChunkStreamHandler delegate) {
		this.chuckLength = chuckLength;
		this.fileEntryState = fileEntryState;
		this.delegate = delegate;
	}

	@Override
	public void handle(InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		
		delegate.initialize(in, executor);

		FileChunkSplitter fileChunkSplitter = delegate.getFileChunkSplitter();

		byte[] buffer = new byte[Math.min(8192 * 16, chuckLength)];

		ByteArrayOutputStream bout = new ByteArrayOutputStream(chuckLength);
		
		int chunkNumber = 0;
		
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
				fileEntryState.increment();
				
				handleChunk(new ByteArrayInputStream(byteArray), executor, false, chunkNumber);
				
				break;
			} else {
				int index = fileChunkSplitter.getNextChunkIndex(byteArray, byteArray.length - 1);
				
				if(index == -1) {
					throw new IllegalArgumentException("No newline found in chunk size " + byteArray.length);
				}
				fileEntryState.increment();
				
				handleChunk(new ByteArrayInputStream(byteArray, 0, index), executor, false, chunkNumber);

				// reuse buffer
				bout.reset();
				// write tail
				if(index + 1 < byteArray.length) {
					bout.write(byteArray, index + 1, byteArray.length - index - 1);
				}
				
				chunkNumber++;
			}
		}
	}

	protected void handleChunk(InputStream in, ThreadPoolExecutor executor, boolean consume, int chunkNumber) throws Exception {
		executor.execute(new Runnable() {
			public void run() {
				try {
					delegate.handleChunk(in, executor, chunkNumber);

					fileEntryState.decrement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

}
