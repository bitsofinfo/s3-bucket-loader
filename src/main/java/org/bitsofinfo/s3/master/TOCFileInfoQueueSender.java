package org.bitsofinfo.s3.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.toc.FileInfo;
import org.bitsofinfo.s3.toc.TOCPayload.MODE;
import org.bitsofinfo.s3.toc.TOCQueue;

public class TOCFileInfoQueueSender implements Runnable {
	

	private static final Logger logger = Logger.getLogger(TOCFileInfoQueueSender.class);

	private TOCQueue tocQueue = null;
	private Queue<FileInfo> toConsumeFrom = null;
	private boolean running = true;
	private List<Thread> threads = new ArrayList<Thread>();
	private MODE mode = null;

	public TOCFileInfoQueueSender(MODE mode, TOCQueue tocQueue, int totalThreads, Queue<FileInfo> toConsumeFrom) {
		this.toConsumeFrom = toConsumeFrom;
		this.tocQueue= tocQueue;
		this.mode = mode;
		
		for (int i=0; i<totalThreads; i++) {
			threads.add(new Thread(this));
		}
	}
	
	public void start() {
		logger.debug("Threads started...");
		this.running = true;
		for (Thread t : threads) {
			t.start();
		}
	}
	
	public void destroy() {
		logger.debug("Destroy...");
		this.running = false;
	}
	
	public void run() {
		Random rand = new Random();
		while (running) {
			try {
				FileInfo finfo = toConsumeFrom.poll();
				if (finfo != null) {
					tocQueue.send(finfo, this.mode);
				} else {
					Thread.currentThread().sleep(rand.nextInt(500));
				}
				
			} catch(Exception e) {
				logger.error("Unexpected error: " + e.getMessage(),e);
			}
		}
	}
}
