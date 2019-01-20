package com.github.skjolber.unzip;

public abstract class NewlineChunkSplitter implements FileChunkSplitter {

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

}
