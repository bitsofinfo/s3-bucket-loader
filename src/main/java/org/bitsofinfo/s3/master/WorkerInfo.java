package org.bitsofinfo.s3.master;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bitsofinfo.s3.control.CCMode;
import org.bitsofinfo.s3.control.CCPayload;
import org.bitsofinfo.s3.control.CCPayloadType;
import org.bitsofinfo.s3.worker.ResultSummary;

import com.google.gson.Gson;

public class WorkerInfo {

	private String hostId = null;
	private CCMode currentMode = null;
	private String ip = null;
	private int totalWritten = 0;
	private int totalValidated = 0;
	private int totalWriteFailures = 0;
	private int totalValidateFailures = 0;
	
	private Map<Date,CCPayload> payloadsReceived = new HashMap<Date,CCPayload>();
	private List<CCPayload> orderedPayloadsReceived = new ArrayList<CCPayload>();
	
	private Gson gson = new Gson();

	public WorkerInfo(String hostId, String ip) {
		super();
		this.hostId = hostId;
		this.ip = ip;
	}
	
	public String getHostId() {
		return hostId;
	}
	
	public String getIP() {
		return ip;
	}
	
	public int getTotalWritten() {
		return totalWritten;
	}
	public int getTotalValidated() {
		return totalValidated;
	}
	
	public CCMode getCurrentMode() {
		return this.currentMode;
	}
	
	public void setCurrentMode(CCMode mode) {
		this.currentMode = mode;
	}
	
	public void addPayloadReceived(CCPayload payload) {
		
		if (!payload.sourceHostId.trim().equalsIgnoreCase(this.hostId)) {
			throw new RuntimeException("cannot add payload received for host other than what this " +
					"WorkInfo is configured for! me:"+this.hostId + " payload:"+payload.sourceHostId);
		}
		this.payloadsReceived.put(new Date(), payload);
		this.orderedPayloadsReceived.add(payload);
		
		if (payload.type == CCPayloadType.WORKER_CURRENT_MODE) {
			this.currentMode = CCMode.valueOf(payload.value.toString());
		}
		
		
		if (payload.type == CCPayloadType.WORKER_WRITES_FINISHED_SUMMARY) {
			ResultSummary writeSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			this.totalWritten = writeSummary.total;
			this.totalWriteFailures = writeSummary.failed;
		}
		
		if (payload.type == CCPayloadType.WORKER_VALIDATIONS_FINISHED_SUMMARY) {
			ResultSummary validateSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			this.totalValidated = validateSummary.total;
			this.totalValidateFailures = validateSummary.failed;
		}
		
		
		if (payload.type == CCPayloadType.WORKER_VALIDATIONS_CURRENT_SUMMARY) {
			ResultSummary validateSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			this.totalValidated = validateSummary.total;
			this.totalValidateFailures = validateSummary.failed;
		}
		
		if (payload.type == CCPayloadType.WORKER_WRITES_CURRENT_SUMMARY) {
			ResultSummary writeSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			this.totalWritten = writeSummary.total;
			this.totalWriteFailures = writeSummary.failed;
		}
	}
	
	public Map<Date, CCPayload> getPayloadsReceived() {
		return payloadsReceived;
	}
	
	public void setPayloadsReceived(Map<Date, CCPayload> payloadsReceived) {
		this.payloadsReceived = payloadsReceived;
	}
	
	public CCPayload getLastPayloadReceived() {
		if (orderedPayloadsReceived.size() > 0) {
			return orderedPayloadsReceived.get(orderedPayloadsReceived.size()-1);
		}
		return null;
	}
	
	
	public boolean writingCurrentSummaryReceived() {
		if (payloadReceived(CCPayloadType.WORKER_WRITES_CURRENT_SUMMARY)) {
			return true;
		}
		
		return false;
	}
	
	
	public boolean validationsCurrentSummaryReceived() {
		if (payloadReceived(CCPayloadType.WORKER_VALIDATIONS_CURRENT_SUMMARY)) {
			return true;
		}
		
		return false;
	}

	
	public boolean writingIsComplete() {
		if (payloadReceived(CCPayloadType.WORKER_WRITES_FINISHED_SUMMARY)) {
			return true;
		}
		
		return false;
	}
	
	public boolean writeSummaryHasFailures() {
		if (payloadReceived(CCPayloadType.WORKER_WRITES_FINISHED_SUMMARY)) {
			CCPayload payload = getPayload(CCPayloadType.WORKER_WRITES_FINISHED_SUMMARY);
			ResultSummary writeSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			if (writeSummary.failed > 0) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean validationSummaryHasFailures() {
		if (payloadReceived(CCPayloadType.WORKER_VALIDATIONS_FINISHED_SUMMARY)) {
			CCPayload payload = getPayload(CCPayloadType.WORKER_VALIDATIONS_FINISHED_SUMMARY);
			ResultSummary validationsSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			if (validationsSummary.failed > 0) {
				return true;
			}
		}
		
		return false;
	}
	
	
	public boolean writeCurrentSummaryHasFailures() {
		if (payloadReceived(CCPayloadType.WORKER_WRITES_CURRENT_SUMMARY)) {
			CCPayload payload = getPayload(CCPayloadType.WORKER_WRITES_CURRENT_SUMMARY);
			ResultSummary writeSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			if (writeSummary.failed > 0) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean validationCurrentSummaryHasFailures() {
		if (payloadReceived(CCPayloadType.WORKER_VALIDATIONS_CURRENT_SUMMARY)) {
			CCPayload payload = getPayload(CCPayloadType.WORKER_VALIDATIONS_CURRENT_SUMMARY);
			ResultSummary validationsSummary = gson.fromJson(payload.value.toString(), ResultSummary.class);
			if (validationsSummary.failed > 0) {
				return true;
			}
		}
		
		return false;
	}
	
	
	public boolean validationIsComplete() {
		if (payloadReceived(CCPayloadType.WORKER_VALIDATIONS_FINISHED_SUMMARY)) {
			return true;
		}
		
		return false;
	}
	
	public boolean errorReportIsReceived() {
		if (payloadReceived(CCPayloadType.WORKER_ERROR_REPORT_DETAILS)) {
			return true;
		}
		
		return false;
	}
	
	public boolean payloadReceived(CCPayloadType type) {
		return getPayload(type) != null;
	}
	
	public CCPayload getPayload(CCPayloadType type) {
		for (CCPayload payload: payloadsReceived.values()) {
			if (payload.type == type) {
				return payload;
			}
		}
		return null;
		
	}
	
	public Object getPayloadValue(CCPayloadType type) {
		CCPayload payload = getPayload(type);
		if (payload != null) {
			return payload.value;
		}
		return null;
		
	}


}
