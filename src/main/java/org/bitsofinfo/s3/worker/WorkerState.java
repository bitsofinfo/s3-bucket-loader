package org.bitsofinfo.s3.worker;

import java.util.ArrayList;
import java.util.List;

import org.bitsofinfo.s3.cmd.FilePathOpResult;
import org.bitsofinfo.s3.control.CCMode;

public class WorkerState {
	
	private String workerHostSourceId = null;
	private String workerIP = null;
	private CCMode currentMode = null;
	private List<FilePathOpResult> filePathsWritten = new ArrayList<FilePathOpResult>();
	private List<FilePathOpResult> filePathsValidated =new ArrayList<FilePathOpResult>();
	private List<FilePathOpResult> filePathsWriteFailures = new ArrayList<FilePathOpResult>();
	private List<FilePathOpResult> filePathValidateFailures = new ArrayList<FilePathOpResult>();

	public WorkerState(String workerHostSourceId, String workerIP) {
		super();
		this.workerHostSourceId = workerHostSourceId;
		this.workerIP = workerIP;
	}
	
	public String getWorkerHostSourceId() {
		return workerHostSourceId;
	}
	public int getTotalWritesOK() {
		return filePathsWritten.size();
	}
	public int getTotalValidatesOK() {
		return filePathsValidated.size();
	}
	public int getTotalWritesFailed() {
		return filePathsWriteFailures.size();
	}
	public int getTotalValidatesFailed() {
		return filePathValidateFailures.size();
	}
	
	public CCMode getCurrentMode() {
		return this.currentMode;
	}
	
	public void setCurrentMode(CCMode mode) {
		this.currentMode = mode;
	}
	
	public synchronized void addFilePathWritten(FilePathOpResult path) {
		this.filePathsWritten.add(path);
	}
	
	public synchronized void addFilePathValidated(FilePathOpResult path) {
		this.filePathsValidated.add(path);
	}
	
	public synchronized void addFilePathValidateFailure(FilePathOpResult path) {
		this.filePathValidateFailures.add(path);
	}
	
	public synchronized void addFilePathWriteFailure(FilePathOpResult path) {
		this.filePathsWriteFailures.add(path);
	}
	
	public int getTotalWritesProcessed() {
		return getTotalWritesFailed() + getTotalWritesOK();
	}
	
	public int getTotalValidationsProcessed() {
		return getTotalValidatesFailed() + getTotalValidatesOK();
	}

	public List<FilePathOpResult> getFilePathsWriteFailures() {
		return filePathsWriteFailures;
	}

	public void setFilePathsWriteFailures(
			List<FilePathOpResult> filePathsWriteFailures) {
		this.filePathsWriteFailures = filePathsWriteFailures;
	}

	public List<FilePathOpResult> getFilePathValidateFailures() {
		return filePathValidateFailures;
	}

	public void setFilePathValidateFailures(
			List<FilePathOpResult> filePathValidateFailures) {
		this.filePathValidateFailures = filePathValidateFailures;
	}

	public String getWorkerIP() {
		return workerIP;
	}
	
}
