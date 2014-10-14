package org.bitsofinfo.s3.worker;

public class ResultSummary {

	public int ok;
	public int failed;
	public int errorsTolerated;
	public int total;
	
	
	public ResultSummary(int ok, int failed, int errorsTolerated, int total) {
		super();
		this.ok = ok;
		this.failed = failed;
		this.errorsTolerated = errorsTolerated;
		this.total = total;
	}
}
