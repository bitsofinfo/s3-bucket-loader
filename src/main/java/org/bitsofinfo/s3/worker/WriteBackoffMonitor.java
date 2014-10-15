package org.bitsofinfo.s3.worker;

public interface WriteBackoffMonitor {

	public boolean writesShouldBackoff();
	public void destroy();
	public void start();
	
}
