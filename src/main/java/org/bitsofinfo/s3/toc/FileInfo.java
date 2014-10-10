package org.bitsofinfo.s3.toc;

public class FileInfo {

	public String filePath = null;
	public long size = 0;
	
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	
	public FileInfo(String filePath, long size) {
		super();
		this.filePath = filePath;
		this.size = size;
	}

}
