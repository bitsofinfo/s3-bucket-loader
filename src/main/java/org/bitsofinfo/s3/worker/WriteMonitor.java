package org.bitsofinfo.s3.worker;

public interface WriteMonitor {

	public boolean writesAreComplete();
	public void destroy();
	public void start();
	
}
