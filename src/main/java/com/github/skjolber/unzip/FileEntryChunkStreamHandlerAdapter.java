package com.github.skjolber.unzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;

public class FileEntryChunkStreamHandlerAdapter implements FileEntryStreamHandler {

	protected final FileEntryChunkState fileEntryState;
	protected final FileEntryChunkStreamHandler delegate;
	
	public FileEntryChunkStreamHandlerAdapter(FileEntryChunkState fileEntryState, FileEntryChunkStreamHandler delegate) {
		this.fileEntryState = fileEntryState;
		this.delegate = delegate;
	}

	@Override
	public void handle(InputStream in, ThreadPoolExecutor executor, boolean consume) throws Exception {
		
		delegate.initialize(in, executor);

		FileChunkSplitter fileChunkSplitter = delegate.getFileChunkSplitter();

		int chunkNumber = 0;
		
		// for saving the part between max chunk length and actual chunk size
		ByteArrayOutputStream tail = new ByteArrayOutputStream(); 
		
		while(true) {
			// avoid creating extra buffers
			int nextChunkLength = fileChunkSplitter.getNextChunkLength();

			byte[] buffer = new byte[Math.max(nextChunkLength, tail.size())];

			System.arraycopy(tail.toByteArray(), 0, buffer, 0, tail.size()); // replace by tail.writeBytes(buffer); in java 11

			int offset = tail.size();

			tail.reset();
			
			while(nextChunkLength > offset) {
				int read = in.read(buffer, offset, nextChunkLength - offset);
				if(read == -1) {
					// end of file
					fileEntryState.increment();
					
					handleChunk(new ByteArrayInputStream(buffer, 0, offset), executor, false, chunkNumber);

					return;
				}
				offset += read;
			}

			int index = fileChunkSplitter.getNextChunkIndex(buffer, offset - 1);
			
			if(index == -1) {
				throw new IllegalArgumentException("No newline found in chunk size " + offset + ": " + new String(buffer, 0, 1024));
			}
			fileEntryState.increment();
			
			handleChunk(new ByteArrayInputStream(buffer, 0, index), executor, false, chunkNumber);

			// save tail
			if(index + 1 < offset) {
				tail.write(buffer, index + 1, buffer.length - index - 1);
			}
			
			chunkNumber++;
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
