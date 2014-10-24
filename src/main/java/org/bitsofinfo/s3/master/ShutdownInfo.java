package org.bitsofinfo.s3.master;

import java.util.ArrayList;
import java.util.List;

public class ShutdownInfo {

	public String s3LogBucketName = null;
	public String s3LogBucketFolderRoot = null;
	public List<String> workerLogFilesToUpload = new ArrayList<String>();
	
}
