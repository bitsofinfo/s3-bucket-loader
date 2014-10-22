package org.bitsofinfo.s3.worker;

import java.util.Date;

public class WriteMonitorError {

	private Date timestamp = null;
	private String msg = null;
	
	public WriteMonitorError(Date timestamp, String msg) {
		this.timestamp = timestamp;
		this.msg = msg;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof WriteMonitorError)) {
			return false;
		}
		
		WriteMonitorError other = (WriteMonitorError)o;
		if (other.getTimestamp().equals(this.timestamp)) {
			return true;
		}
		
		return false;
	}
	
	public int hashCode() {
		return timestamp.hashCode();
	}
	
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}

}
