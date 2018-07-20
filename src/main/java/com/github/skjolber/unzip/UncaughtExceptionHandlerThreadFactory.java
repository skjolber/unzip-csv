package com.github.skjolber.unzip;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

public class UncaughtExceptionHandlerThreadFactory implements ThreadFactory {

	private class UncaughtExceptionHandlerDelegate implements UncaughtExceptionHandler {
		
		private final UncaughtExceptionHandler delegate;

		public UncaughtExceptionHandlerDelegate(UncaughtExceptionHandler delegate) {
			this.delegate = delegate;
		}

		public void uncaughtException(Thread t, Throwable e) {
			handler.uncaughtException(t, e);
			
			delegate.uncaughtException(t, e);
		}
	}
	
	protected ThreadFactory delegate;
	protected UncaughtExceptionHandler handler;
	
	public UncaughtExceptionHandlerThreadFactory(ThreadFactory delegate, UncaughtExceptionHandler handler) {
		super();
		this.delegate = delegate;
		this.handler = handler;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread newThread = delegate.newThread(r);
		
		UncaughtExceptionHandler originalUncaughtExceptionHandler = newThread.getUncaughtExceptionHandler();
		if(originalUncaughtExceptionHandler == null) {
			newThread.setUncaughtExceptionHandler(handler);
		} else {
			newThread.setUncaughtExceptionHandler(new UncaughtExceptionHandlerDelegate(originalUncaughtExceptionHandler));
		}
		
		return newThread;
	}
	
}
