package org.bitsofinfo.s3.yas3fs;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.worker.WriteMonitor;

/**
 * Monitors the Yas3fs log for entries like this looking for the s3_queue being zero
 * meaning that there are no uploads to s3 in progress
 * 
 * INFO entries, mem_size, disk_size, download_queue, prefetch_queue, s3_queue: 1, 0, 0, 0, 0, 0
 *	
 * @author bitsofinfo
 *
 */
public class Yas3fsS3UploadMonitor implements WriteMonitor, Runnable {
	
	private static final Logger logger = Logger.getLogger(Yas3fsS3UploadMonitor.class);

	private long checkEveryMS = 10000;
	private int isIdleWhenNZeroUploads = 0; // count of the total number of s3UploadCounts entries must be ZERO to declare we are idel
	
	private String pathToLogFile = null;
	private boolean running = true;
	private Thread monitorThread = null;
	private String latestLogTail = null;
	
	private Stack<Integer> s3UploadCounts = new Stack<Integer>();
	
	public Yas3fsS3UploadMonitor() {}
	
	public Yas3fsS3UploadMonitor(String pathToLogFile, long checkEveryMS, int isIdleWhenNZeroUploads) {
		this.pathToLogFile = pathToLogFile;	
		this.checkEveryMS = checkEveryMS;
		this.isIdleWhenNZeroUploads = isIdleWhenNZeroUploads;
	}
	
	public void start() {
		monitorThread = new Thread(this);
		monitorThread.start();
	}
	
	public void destroy() {
		this.running = false;
	}

	public void run() {
		while(running) {
			try {
				
				Thread.currentThread().sleep(this.checkEveryMS);
				
				RandomAccessFile file = new RandomAccessFile(new File(pathToLogFile), "r");
				byte[] buffer = new byte[10240];
				if (file.length() >= buffer.length) {
					file.seek(file.length()-buffer.length);
				}
				file.read(buffer, 0, buffer.length);	
				file.close();
				
				this.latestLogTail = new String(buffer,"UTF-8");
				
			} catch(Exception e) {
				logger.error("Unexpected error tailing yas3fs.log: " + this.pathToLogFile + " " + e.getMessage(),e);
			}
		}
	}
	
	public int getS3UploadQueueSize() {
		if (this.latestLogTail != null) {
			System.out.println(latestLogTail); 
			Pattern s3QueueSizePatten = Pattern.compile(".+s3_queue: \\d+, \\d+, \\d+, \\d+, \\d+, (\\d+).*");
			Matcher m = s3QueueSizePatten.matcher(this.latestLogTail);
			int lastMatch = -1;
			
			while (m.find()) {
			    lastMatch = Integer.valueOf(m.group(1).trim());
			}
			
			return lastMatch;
		}
		
		return -1;
	}
	
	public boolean writesAreComplete() {
		// get the latest s3upload queue size
		int s3UploadQueueSize = this.getS3UploadQueueSize();
		
		// add it to our list (most recent -> oldest)
		this.s3UploadCounts.push(s3UploadQueueSize);
		
		int count = -1;
		
		// if we have enought upload count history...
		if (this.s3UploadCounts.size() > this.isIdleWhenNZeroUploads) {
			
			// clone it
			Stack<Integer> toScan = (Stack<Integer>)this.s3UploadCounts.clone();
			
			// look through N past upload counts we have checked
			// and add them all up... (the stack is a LIFO stack)
			// so most recent -> oldest
			count = 0; // init to zero....
			for (int i=0; i<this.isIdleWhenNZeroUploads; i++) {
				count += toScan.pop();
			}
			
			// if they all add up to ZERO, then yas3fs is not uploading anymore.
			if (count == 0) {
				logger.debug("writesAreComplete() YES: count = 0");
				return true;
			}
		}
		
		
		logger.debug("writesAreComplete() NO: count = " + count);
		return false;
		
		
	}

	public void setCheckEveryMS(long checkEveryMS) {
		this.checkEveryMS = checkEveryMS;
	}

	public void setIsIdleWhenNZeroUploads(int isIdleWhenNZeroUploads) {
		this.isIdleWhenNZeroUploads = isIdleWhenNZeroUploads;
	}

	public void setPathToLogFile(String pathToLogFile) {
		this.pathToLogFile = pathToLogFile;
	}

}
