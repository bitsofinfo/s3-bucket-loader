package org.bitsofinfo.s3.toc;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.cmd.TocPathOpResult;
import org.bitsofinfo.s3.worker.WorkerState;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.StorageClass;

public class S3KeyCopyingTOCPayloadHandler implements ProgressListener, TOCPayloadHandler {

	private static final Logger logger = Logger.getLogger(S3KeyCopyingTOCPayloadHandler.class);
	
	private String sourceS3BucketName = null;
	private String targetS3BucketName = null;
	private StorageClass storageClass = null;
	private boolean enableServerSideEncryption = false;
	private AmazonS3Client s3Client = null;

	@Override
	public void destroy() {
		// nothing to do
	}
	
	@Override
	public void progressChanged(ProgressEvent progressEvent) {
		logger.debug("progressChanged() " +progressEvent.getEventType() + 
				       " bytes:" + progressEvent.getBytes() + 
				      " bytesTransferred: " + progressEvent.getBytesTransferred());
	}
	

	@Override
	public void handlePayload(TOCPayload payload) throws Exception {
		throw new UnsupportedOperationException("S3KeyCopyingTOCPayloadHandler does not " +
				"support this method variant, call me through Worker");
	}

	@Override
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception {
		
		TocInfo tocInfo = payload.tocInfo;
		
		String logPrefix = "handlePayload() KeyCopy s3://" + this.sourceS3BucketName + "/" + tocInfo.path + 
				" => s3://" + this.targetS3BucketName +"/"+ tocInfo.path;
		
		try {
		
			CopyObjectRequest copyRequest = new CopyObjectRequest(this.sourceS3BucketName, 
																  tocInfo.path, 
																  this.targetS3BucketName, 
																  tocInfo.path);
			copyRequest.setStorageClass(storageClass);
			// copyRequest.setGeneralProgressListener(this);
			
			if (this.enableServerSideEncryption) {
				copyRequest.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
			}
			
			CopyObjectResult copyResult = s3Client.copyObject(copyRequest);
			
			logger.debug(logPrefix + " copied OK");
			workerState.addTocPathWritten(new TocPathOpResult(payload.mode, true, tocInfo.path, "s3.copyKey", "OK"));
			
		} catch(Exception e) {
			logger.error(logPrefix + " unexpected ERROR: " + e.getMessage(),e);
			workerState.addTocPathWriteFailure(
					new TocPathOpResult(payload.mode, false, tocInfo.path, "s3.copyKey", logPrefix + " " + e.getMessage()));
		}
		
	}

	public String getSourceS3BucketName() {
		return sourceS3BucketName;
	}

	public void setSourceS3BucketName(String sourceS3BucketName) {
		this.sourceS3BucketName = sourceS3BucketName;
	}

	public String getTargetS3BucketName() {
		return targetS3BucketName;
	}

	public void setTargetS3BucketName(String targetS3BucketName) {
		this.targetS3BucketName = targetS3BucketName;
	}

	public StorageClass getStorageClass() {
		return storageClass;
	}

	public void setStorageClass(StorageClass storageClass) {
		this.storageClass = storageClass;
	}

	public boolean isEnableServerSideEncryption() {
		return enableServerSideEncryption;
	}

	public void setEnableServerSideEncryption(boolean enableServerSideEncryption) {
		this.enableServerSideEncryption = enableServerSideEncryption;
	}

	public AmazonS3Client getS3Client() {
		return s3Client;
	}

	public void setS3Client(AmazonS3Client s3Client) {
		this.s3Client = s3Client;
	}

}
