package org.bitsofinfo.s3.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.toc.TOCQueue;

public class TOCQueueEmptier implements Runnable {
	

	private static final Logger logger = Logger.getLogger(TOCQueueEmptier.class);

	private TOCQueue tocQueue = null;
	private boolean running = true;
	private List<Thread> threads = new ArrayList<Thread>();

	public TOCQueueEmptier(TOCQueue tocQueue, int totalThreads) {
		this.tocQueue= tocQueue;
		
		for (int i=0; i<totalThreads; i++) {
			threads.add(new Thread(this));
		}
	}
	
	public boolean isRunning() {
		return running;
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
		int numberOfZeroRemoved = 0;
		
		while (running) {
			try {
				int totalRemoved = this.tocQueue.emptyTOCQueue();
				if (totalRemoved == 0) {
					numberOfZeroRemoved++;
					Thread.currentThread().sleep(2000);
				}
				
				if (numberOfZeroRemoved == 10) { // 10 times we did nothing
					destroy();
				}
				
			} catch(Exception e) {
				logger.error("Unexpected error: " + e.getMessage(),e);
			}
		}
		
		logger.debug("I am done running...idle");
	}
}
