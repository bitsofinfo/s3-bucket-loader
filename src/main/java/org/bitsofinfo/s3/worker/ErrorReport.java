package org.bitsofinfo.s3.worker;

import java.util.List;

import org.bitsofinfo.s3.cmd.TocPathOpResult;

public class ErrorReport {

	public List<TocPathOpResult> failedWrites = null;
	public List<TocPathOpResult> failedValidates = null;
	public List<TocPathOpResult> errorsTolerated = null;

}
