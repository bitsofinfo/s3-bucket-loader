package org.bitsofinfo.s3.master;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.toc.SourceTOCGenerator;
import org.bitsofinfo.s3.toc.TocInfo;
import org.bitsofinfo.s3.toc.TOCPayload.MODE;
import org.bitsofinfo.s3.toc.TOCQueue;

public class TOCGeneratorAndSender implements Runnable {

	private static final Logger logger = Logger.getLogger(TOCGeneratorAndSender.class);

	private Thread myThread = new Thread(this);
	private TocInfoQueueSender tocFileInfoQueueSender = null;
	private Queue<TocInfo> tocFileInfoQueue = null;
	private SourceTOCGenerator tocGenerator = null;
	private MODE mode = null;
	private TOCGenerationComplete handler = null;
	private Collection<TocInfo> toc = null;

	public TOCGeneratorAndSender(MODE mode, 
			TOCGenerationComplete handler, 
			TOCQueue tocQueue, 
			int tocDispatchThreadsTotal, 
			Collection<TocInfo> toc) {

		this.handler = handler;
		this.mode = mode;
		this.toc = toc;

		// populate the queue that the "sender" will concurrently consume
		// from while the TOC is being generated
		this.tocFileInfoQueue = new TocInfoSizeAwareQueue(100000000);
		this.tocFileInfoQueue.addAll(this.toc);
		
		this.tocFileInfoQueueSender = new TocInfoQueueSender(mode, tocQueue, tocDispatchThreadsTotal, this.tocFileInfoQueue);

	}

	public TOCGeneratorAndSender(MODE mode, 
			TOCGenerationComplete handler, 
			TOCQueue tocQueue, 
			int tocDispatchThreadsTotal, 
			SourceTOCGenerator tocGenerator) {

		this.tocGenerator = tocGenerator;
		this.handler = handler;
		this.mode = mode;

		// generate a queue that the "sender" will concurrently consume
		// from while the TOC is being generated
		this.tocFileInfoQueue = new TocInfoSizeAwareQueue(100000000);
		
		this.tocFileInfoQueueSender = new TocInfoQueueSender(mode, tocQueue, tocDispatchThreadsTotal, this.tocFileInfoQueue);

	}

	public void generateAndSendTOC() {
		logger.debug("Thread started...");
		myThread.start();	
	}

	public void destroy() {
		logger.debug("Destroy...");

		try {
			myThread.interrupt();
		} catch(Exception ignore){}

		try {
			this.tocFileInfoQueueSender.destroy();
		} catch(Exception ignore){}


	}
	
	private Collection<TocInfo> getTOC() throws Exception {
		if (this.toc == null) {
			this.toc = tocGenerator.generateTOC(tocFileInfoQueue);
		}
		
		return toc;
	}

	public void run() {
		try {
			// generate and get all TOC messages (write live to the queue we just created)
			tocFileInfoQueueSender.start(); // start the consumer

			logger.info("run("+mode+") generating TOC...");
			Collection<TocInfo> toc = getTOC();

			// set on handler
			handler.tocGenerationComplete(toc);

			// while the queue is not empty, sleep....
			while (tocFileInfoQueue.size() > 0) {
				logger.debug("TOC generation complete, waiting for TOCFileInfoQueueSender" +
						" thread to complete sending to SQS.. size:" + tocFileInfoQueue.size());
				Thread.currentThread().sleep(10000);
			}

			// queue is empty.. stop it (i.e. tocFileInfoQueueSender is done consuming all from it)
			tocFileInfoQueueSender.destroy();

			logger.info("TOCGeneratorAndSender(MODE="+mode+") done sending " + toc.size() + " tocPaths over TOCQueue....");

		} catch(InterruptedException e) {
			logger.warn("Caught InterruptedException, stopping TOC generation!");

		} catch(Exception e) {
			logger.error("Error generating TOC: " + e.getMessage(),e);
		}
	}
}
