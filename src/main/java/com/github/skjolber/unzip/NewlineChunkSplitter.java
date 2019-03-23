package com.github.skjolber.unzip;

public class NewlineChunkSplitter implements FileChunkSplitter {

	protected int chunkLength;
	
	public NewlineChunkSplitter(int chunkLength) {
		this.chunkLength = chunkLength;
	}

	@Override
	public int getNextChunkIndex(byte[] bytes, int index) {
		// seek backward for a newline
		while(index >= 0) {
			if(bytes[index] == '\n') {
				break;
			}
			index--;
		}
		return index;
	}

	@Override
	public int getNextChunkLength() {
		return chunkLength;
	}
}
