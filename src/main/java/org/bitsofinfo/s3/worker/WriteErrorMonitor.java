package org.bitsofinfo.s3.worker;

import java.util.Set;

public interface WriteErrorMonitor {

	public Set<WriteMonitorError> getWriteErrors();
	public void destroy();
	public void start();
	
}
