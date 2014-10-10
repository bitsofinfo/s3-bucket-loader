package org.bitsofinfo.s3.worker;

import java.util.List;

import org.bitsofinfo.s3.cmd.FilePathOpResult;

public class ErrorReport {

	public List<FilePathOpResult> failedWrites = null;
	public List<FilePathOpResult> failedValidates = null;

}
