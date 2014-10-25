package org.bitsofinfo.s3.worker;

import java.util.List;
import java.util.Set;

import org.bitsofinfo.s3.cmd.TocPathOpResult;

public class ErrorReport {

	public String id = null;
	public String ip = null;
	public List<TocPathOpResult> failedWrites = null;
	public List<TocPathOpResult> failedValidates = null;
	public List<TocPathOpResult> errorsTolerated = null;
	public Set<WriteMonitorError> writeMonitorErrors = null;
	public List<TocPathOpResult> failedPostWriteLocalValidates = null;

}
