package org.bitsofinfo.s3.toc;

public class TOCPayload {

	public static enum MODE {WRITE, VALIDATE}
	
	public MODE mode = null;
	public TocInfo tocInfo = null;
	
}
