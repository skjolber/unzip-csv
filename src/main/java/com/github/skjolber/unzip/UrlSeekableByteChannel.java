package com.github.skjolber.unzip;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class UrlSeekableByteChannel implements SeekableByteChannel {

    private final AtomicBoolean closed = new AtomicBoolean();
    private int position;

    private UrlByteChannelCache cache;

	public UrlSeekableByteChannel(URL url, int chunkLength) {
		this(url, chunkLength, -1);
	}

	public UrlSeekableByteChannel(URL url, int chunkLength, int concurrentConnections) {
		this(new UrlByteChannelCache(url, chunkLength, concurrentConnections));
	}
	
    public UrlSeekableByteChannel(UrlByteChannelCache cache) {
		this.cache = cache;
	}

	@Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen();
        if (newPosition < 0L || newPosition > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Position has to be in range 0.. " + Integer.MAX_VALUE);
        }
        position = (int) newPosition;
        return this;
    }

    @Override
    public long position() {
        return position;
    }
    
    @Override
    public SeekableByteChannel truncate(long newSize) {
    	throw new RuntimeException();
    }
    
    @Override
    public int read(ByteBuffer buf) throws IOException {
        ensureOpen();
        repositionIfNecessary();
        int wanted = buf.remaining();
        int possible = cache.size() - position;
        if (possible <= 0) {
            return -1;
        }
        if (wanted > possible) {
            wanted = possible;
        }
        
        int read = cache.put(buf, position, wanted);
    	
        position += read;
        return read;
    }


	@Override
    public void close() {
        closed.set(true);
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public int write(ByteBuffer b) throws IOException {
    	throw new RuntimeException();
    }

    private void ensureOpen() throws ClosedChannelException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
    }    
    
    private void repositionIfNecessary() throws IOException {
        if (position > cache.size()) {
            position = cache.size();
        }
    }

	@Override
	public long size() throws IOException {
		return cache.size();
	}

}
