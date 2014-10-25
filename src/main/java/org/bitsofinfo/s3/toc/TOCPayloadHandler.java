package org.bitsofinfo.s3.toc;

import org.bitsofinfo.s3.worker.WorkerState;

public interface TOCPayloadHandler {

	public void destroy();
	public void handlePayload(TOCPayload payload) throws Exception;
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception;
	
}
