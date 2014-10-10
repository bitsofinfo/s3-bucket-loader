package org.bitsofinfo.s3.worker;

public class ResultSummary {

	public int ok;
	public int failed;
	public int total;
	
	public ResultSummary(int ok, int failed, int total) {
		super();
		this.ok = ok;
		this.failed = failed;
		this.total = total;
	}
}
