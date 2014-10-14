package org.bitsofinfo.s3.toc;

import java.io.File;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.cmd.TocPathOpResult;
import org.bitsofinfo.s3.worker.WorkerState;

public class ValidatingTOCPayloadHandler implements TOCPayloadHandler {

	private static final Logger logger = Logger.getLogger(ValidatingTOCPayloadHandler.class);
	
	private String targetDirectoryRootPath = null;
	
	public ValidatingTOCPayloadHandler() {
	}
	
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception {
		String targetPath = null;
		
		try {
			targetPath = (targetDirectoryRootPath + payload.tocInfo.getPath()).replaceAll("//", "/");
			
			File toCheck = new File(targetPath);
			
			if (payload.tocInfo.isDirectory() && toCheck.exists() && toCheck.isDirectory()) {
				
				workerState.addTocPathValidated(
						new TocPathOpResult(payload.mode, true, targetPath, "io.File.[dir].exists()", "ok"));
				
			} else if (!payload.tocInfo.isDirectory() && toCheck.exists() && toCheck.length() == payload.tocInfo.size) {
				
				workerState.addTocPathValidated(
						new TocPathOpResult(payload.mode, true, targetPath, "io.File.exists() + size", "ok"));
				
			} else {
				logger.error("File validation failed, does not exist! " + targetPath);

				workerState.addTocPathValidateFailure(
						new TocPathOpResult(payload.mode, false, targetPath, "io.File.exists()", "!exists"));
			}
			
		} catch(Exception e) {
			
			workerState.addTocPathValidateFailure(
					new TocPathOpResult(payload.mode, false, targetPath, "io.File.exists()", "exception: " + e.getMessage()));
			
			logger.error("File validation exception: " + e.getMessage(),e);
		}
	}

	
	public void setTargetDirectoryRootPath(String targetDirectoryRootPath) {
		this.targetDirectoryRootPath = targetDirectoryRootPath;
	}

	
	public void handlePayload(TOCPayload payload) throws Exception {
		throw new UnsupportedOperationException("ValidatingTOCPayloadHandler does not " +
				"support this method variant, call me through Worker");
	}

}
