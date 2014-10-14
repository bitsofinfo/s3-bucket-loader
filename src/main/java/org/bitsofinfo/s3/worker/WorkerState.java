package org.bitsofinfo.s3.worker;

import java.util.ArrayList;
import java.util.List;

import org.bitsofinfo.s3.cmd.TocPathOpResult;
import org.bitsofinfo.s3.control.CCMode;

public class WorkerState {
	
	private String workerHostSourceId = null;
	private String workerIP = null;
	private CCMode currentMode = null;
	private List<TocPathOpResult> tocPathsErrorsTolerated = new ArrayList<TocPathOpResult>();
	private List<TocPathOpResult> tocPathsWritten = new ArrayList<TocPathOpResult>();
	private List<TocPathOpResult> tocPathsValidated =new ArrayList<TocPathOpResult>();
	private List<TocPathOpResult> tocPathsWriteFailures = new ArrayList<TocPathOpResult>();
	private List<TocPathOpResult> tocPathsValidateFailures = new ArrayList<TocPathOpResult>();

	public WorkerState(String workerHostSourceId, String workerIP) {
		super();
		this.workerHostSourceId = workerHostSourceId;
		this.workerIP = workerIP;
	}
	
	public String getWorkerHostSourceId() {
		return workerHostSourceId;
	}
	public int getTotalWritesOK() {
		return tocPathsWritten.size();
	}
	public int getTotalValidatesOK() {
		return tocPathsValidated.size();
	}
	public int getTotalWritesFailed() {
		return tocPathsWriteFailures.size();
	}
	public int getTotalValidatesFailed() {
		return tocPathsValidateFailures.size();
	}
	public int getTotalErrorsTolerated() {
		return tocPathsErrorsTolerated.size();
	}
	
	public CCMode getCurrentMode() {
		return this.currentMode;
	}
	
	public void setCurrentMode(CCMode mode) {
		this.currentMode = mode;
	}
	
	public synchronized void addTocPathErrorTolerated(TocPathOpResult path) {
		this.tocPathsErrorsTolerated.add(path);
	}
	
	public synchronized void addTocPathWritten(TocPathOpResult path) {
		this.tocPathsWritten.add(path);
	}
	
	public synchronized void addTocPathValidated(TocPathOpResult path) {
		this.tocPathsValidated.add(path);
	}
	
	public synchronized void addTocPathValidateFailure(TocPathOpResult path) {
		this.tocPathsValidateFailures.add(path);
	}
	
	public synchronized void addTocPathWriteFailure(TocPathOpResult path) {
		this.tocPathsWriteFailures.add(path);
	}
	
	public int getTotalWritesProcessed() {
		// note we do not include "tolerated" here because they are part of the OKs
		// and the "tolerated" stuff is just supplemental information
		return getTotalWritesFailed() + getTotalWritesOK();
	}
	
	public int getTotalValidationsProcessed() {
		return getTotalValidatesFailed() + getTotalValidatesOK();
	}

	public List<TocPathOpResult> getTocPathsWriteFailures() {
		return tocPathsWriteFailures;
	}

	public List<TocPathOpResult> getTocPathsErrorsTolerated() {
		return tocPathsErrorsTolerated;
	}

	public void setTocPathsWriteFailures(
			List<TocPathOpResult> tocPathsWriteFailures) {
		this.tocPathsWriteFailures = tocPathsWriteFailures;
	}

	public List<TocPathOpResult> getTocPathValidateFailures() {
		return tocPathsValidateFailures;
	}

	public void setTocPathValidateFailures(
			List<TocPathOpResult> filePathValidateFailures) {
		this.tocPathsValidateFailures = filePathValidateFailures;
	}

	public String getWorkerIP() {
		return workerIP;
	}
	
}
