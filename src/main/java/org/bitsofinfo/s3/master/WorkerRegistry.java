package org.bitsofinfo.s3.master;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.control.CCMode;
import org.bitsofinfo.s3.control.CCPayload;

public class WorkerRegistry {

	private static final Logger logger = Logger.getLogger(WorkerRegistry.class);
	
	
	private Map<String,WorkerInfo> registry = new TreeMap<String,WorkerInfo>(String.CASE_INSENSITIVE_ORDER);


	public WorkerInfo getWorkerByIP(String ip) {
		for (WorkerInfo info : registry.values()) {
			if (info.getIP().equals(ip)) {
				return info;
			}
		}
		
		return null;
	}
	
	public WorkerInfo register(String workerHostId, String workerIP) {
		if (getWorkerInfo(workerHostId) == null) {
			WorkerInfo newWorker = new WorkerInfo(workerHostId.trim(), workerIP);
			registry.put(workerHostId.trim(), newWorker);
			logger.trace("Registered worker: " + workerHostId + " ip:"+ workerIP);
		}
		return getWorkerInfo(workerHostId);
	}
	
	public void registerWorkerPayload(CCPayload payload) {
		if (payload.fromMaster) {
			throw new RuntimeException("Cannot call registerWorkerPaylod with one where fromMaster=true");
		}
		
		WorkerInfo workerInfo = getWorkerInfo(payload.sourceHostId);
		if (workerInfo == null) {
			workerInfo = register(payload.sourceHostId, payload.sourceHostIP);
		}
		
		workerInfo.addPayloadReceived(payload);
		logger.trace("Registered Worker["+payload.sourceHostId+"] Payload: " + payload.type + " val:" + payload.value);
	}
	
	public Set<String> getWorkerHostnames() {
		return this.registry.keySet();
	}
	
	public int getTotalWritten() {
		int total = 0;
		for (WorkerInfo info : registry.values()) {
			total += info.getTotalWritten();
		}
		return total;
	}
	
	public int size() {
		return registry.size();
	}
	
	public Set<String> getWorkersAwaitingErrorReport() {
		Set<String> awaiting = new HashSet<String>();
		for (WorkerInfo info : registry.values()) {
			if (!info.errorReportIsReceived()) {
				awaiting.add(info.getHostId());
			}
		}
		return awaiting;
	}
	
	
	public Set<String> getWorkersAwaitingWriteReport() {
		Set<String> awaiting = new HashSet<String>();
		for (WorkerInfo info : registry.values()) {
			if (!info.writingIsComplete()) {
				awaiting.add(info.getHostId());
			}
		}
		return awaiting;
	}
	
	public Set<String> getWorkersAwaitingValidationReport() {
		Set<String> awaiting = new HashSet<String>();
		for (WorkerInfo info : registry.values()) {
			if (!info.validationIsComplete()) {
				awaiting.add(info.getHostId());
			}
		}
		return awaiting;
	}
	
	
	public int getTotalValidated() {
		int total = 0;
		for (WorkerInfo info : registry.values()) {
			total += info.getTotalValidated();
		}
		return total;
	}
	
	public WorkerInfo getWorkerInfo(String workerHostname) {
		return registry.get(workerHostname.trim());
	}
	
	public boolean allWorkerErrorReportsAreIn() {
		for (WorkerInfo wi : registry.values()) {
			if (!wi.errorReportIsReceived()) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean allWorkerWritesAreComplete() {
		for (WorkerInfo wi : registry.values()) {
			if (!wi.writingIsComplete()) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean anyWorkerWritesAreComplete() {
		for (WorkerInfo wi : registry.values()) {
			if (wi.writingIsComplete()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean anyWorkerErrorReportsAreReceived() {
		for (WorkerInfo wi : registry.values()) {
			if (wi.errorReportIsReceived()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean anyWorkerValidatesAreComplete() {
		for (WorkerInfo wi : registry.values()) {
			if (wi.validationIsComplete()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean anyWorkerWritesContainErrors() {
		for (WorkerInfo wi : registry.values()) {
			if (wi.writeSummaryHasFailures()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean anyWorkerValidationsContainErrors() {
		for (WorkerInfo wi : registry.values()) {
			if (wi.validationSummaryHasFailures()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean allWorkerValidatesAreComplete() {
		for (WorkerInfo wi : registry.values()) {
			if (!wi.validationIsComplete()) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean allWorkersCurrentModeIs(CCMode mode) {
		for (WorkerInfo wi : registry.values()) {
			if (wi.getCurrentMode() != mode) {
				return false;
			}
		}
		
		return true;
	}
	
}
