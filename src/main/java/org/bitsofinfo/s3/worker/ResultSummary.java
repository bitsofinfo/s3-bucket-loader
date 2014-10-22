package org.bitsofinfo.s3.worker;

public class ResultSummary {

	public boolean TOCConsumptionPaused = false;
	public int ok;
	public int failed;
	public int errorsTolerated;
	public int writeMonitorErrors;
	public int total;
	
	
	public ResultSummary(boolean TOCConsumptionPaused, int ok, int failed, int errorsTolerated, int writeMonitorErrors, int total) {
		super();
		this.TOCConsumptionPaused = TOCConsumptionPaused;
		this.ok = ok;
		this.failed = failed;
		this.errorsTolerated = errorsTolerated;
		this.writeMonitorErrors = writeMonitorErrors;
		this.total = total;
	}
}
