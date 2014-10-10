package org.bitsofinfo.s3.toc;

public class TocInfo {

	public String path = null;
	public boolean isDirectory = false;
	public long size = 0;
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	
	public TocInfo(String filePath, long size) {
		super();
		this.path = filePath;
		this.size = size;
	}
	
	public boolean isDirectory() {
		return isDirectory;
	}
	
	public void setIsDirectory(boolean isDirectory) {
		this.isDirectory = isDirectory;
	}

}
