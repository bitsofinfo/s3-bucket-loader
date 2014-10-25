package org.bitsofinfo.s3.toc;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.cmd.TocPathOpResult;
import org.bitsofinfo.s3.worker.WorkerState;

import com.amazonaws.services.s3.AmazonS3Client;

public class ValidatingTOCPayloadHandler implements TOCPayloadHandler {

	private static final Logger logger = Logger.getLogger(ValidatingTOCPayloadHandler.class);
	
	private String targetDirectoryRootPath = null;
	
	public static enum MODE { validateEverywhere, validateLocallyOnly, validateS3Only, validateLocallyThenS3OnFailure }
	
	private MODE validateMode = MODE.validateLocallyThenS3OnFailure;
	
	private TOCPayloadValidator validator = new TOCPayloadValidator();
	
	public ValidatingTOCPayloadHandler() {
		
	}
	
	public void destroy() {}
	
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception {

		try {
			
			// VALIDATE LOCAL first, then S3
			if (validateMode == MODE.validateLocallyThenS3OnFailure) {
				
				TocPathOpResult localCheck = validator.validateLocally(payload,this.targetDirectoryRootPath);
				
				if (localCheck.success) {
					workerState.addTocPathValidated(localCheck);

				// failed? check s3
				} else {
					logger.error("validateLocally() failed, falling back to S3 check..." + payload.tocInfo.getPath());
					TocPathOpResult s3Check = validator.validateOnS3(payload);
					
					if (s3Check.success) {
						workerState.addTocPathValidated(s3Check);
						
					// both failed....
					} else {
						workerState.addTocPathValidateFailure(new TocPathOpResult(payload.mode, false, payload.tocInfo.path, 
								"localFS["+localCheck.success+"]_then_s3["+s3Check.success+"]", "failed: s3["+s3Check.message+"] local["+localCheck.message+"]"));
					}
					
				}
				
				return;
			}
			
			// VALIDATE BOTH
			if (validateMode == MODE.validateEverywhere) {
				TocPathOpResult localCheck = validator.validateLocally(payload,this.targetDirectoryRootPath);
				TocPathOpResult s3Check = validator.validateOnS3(payload);
				
				if (localCheck.success && s3Check.success) {
					workerState.addTocPathValidated(new TocPathOpResult(payload.mode, true, payload.tocInfo.path, "localFS_and_s3", "both validated ok"));
				} else {
					workerState.addTocPathValidateFailure(new TocPathOpResult(payload.mode, false, payload.tocInfo.path, 
							"localFS["+localCheck.success+"]_and_s3["+s3Check.success+"]", "failed: s3["+s3Check.message+"] local["+localCheck.message+"]"));
				}
				
				return;
			}
			
			// VALIDATE LOCAL ONLY
			if (validateMode == MODE.validateLocallyOnly) {
				TocPathOpResult localCheck = validator.validateLocally(payload,this.targetDirectoryRootPath);
				
				if (localCheck.success) {
					workerState.addTocPathValidated(localCheck);
				} else {
					workerState.addTocPathValidateFailure(localCheck);
				}
				
				return;
			}
			
			
			// VALIDATE S3 ONLY
			if (validateMode == MODE.validateS3Only) {
				TocPathOpResult s3Check = validator.validateOnS3(payload);
				
				if (s3Check.success) {
					workerState.addTocPathValidated(s3Check);
				} else {
					workerState.addTocPathValidateFailure(s3Check);
				}
				
				return;
			}

			
		} catch(Exception e) {
			
			workerState.addTocPathValidateFailure(
					new TocPathOpResult(payload.mode, false, payload.tocInfo.path, "validation_error", "exception: " + e.getMessage()));
			
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

	public AmazonS3Client getS3Client() {
		return validator.getS3Client();
	}

	public void setS3Client(AmazonS3Client s3Client) {
		validator.setS3Client(s3Client);
	}

	public String getS3BucketName() {
		return validator.getS3BucketName();
	}

	public void setS3BucketName(String s3BucketName) {
		validator.setS3BucketName(s3BucketName);
	}
	

	public MODE getValidateMode() {
		return validateMode;
	}


	public void setValidateMode(MODE validateMode) {
		this.validateMode = validateMode;
	}

}
