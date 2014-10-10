package org.bitsofinfo.s3.control;

public enum CCPayloadType {
	
	MASTER_CURRENT_MODE,  // current mode of the master
	WORKER_CURRENT_MODE,  // current mode of workers
	
	WORKER_WRITES_CURRENT_SUMMARY, 		// sent periodically during write mode stating number of successful/failed so far
	WORKER_VALIDATIONS_CURRENT_SUMMARY, 		// sent periodically during write mode stating number of successful/failed so far

	WORKER_WRITES_FINISHED_SUMMARY,  	// sent by workers when idle and report total WRITE mode messages processed
	WORKER_VALIDATIONS_FINISHED_SUMMARY, // sent by workers when idle and report total VALIDATE mode messages processed
	
	WORKER_ERROR_REPORT_DETAILS,  // sent by workers when REPORT_ERRORS mode is switched on
	
	CMD_WORKER_SHUTDOWN   // sent when master to tell worker to shutdown

}
