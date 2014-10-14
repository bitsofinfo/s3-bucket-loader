package org.bitsofinfo.s3.cmd;

import org.bitsofinfo.s3.toc.TOCPayload;

public class TocPathOpResult {

	public boolean success;
	public String filePath;
	public String operation;
	public String message;
	public TOCPayload.MODE mode;
	
	
	public TocPathOpResult(TOCPayload.MODE mode, boolean success, String filePath, String operation, String message) {
		super();
		this.success = success;
		this.filePath = filePath;
		this.operation = operation;
		this.message = message;
		this.mode = mode;
	}
	
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

	public TOCPayload.MODE getMode() {
		return mode;
	}

	public void setMode(TOCPayload.MODE mode) {
		this.mode = mode;
	}


}
