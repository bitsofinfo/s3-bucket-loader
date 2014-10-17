	package org.bitsofinfo.s3.toc;

import java.io.File;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;


/**
 * Returns all unique directory and file paths, also logs every 30s
 * the current number of items collected
 * 
 * @author inter0p
 *
 */
public class DirectoryCrawler implements SourceTOCGenerator, Runnable {
	
	private static final Logger logger = Logger.getLogger(DirectoryCrawler.class);

	private File rootDir = null;
	private boolean running = true;
	private int tocInfosGenerated = 0;
	
	public DirectoryCrawler() {}
	
	public DirectoryCrawler(File rootDir) {
		this.rootDir = rootDir;
	}
	
	public void setRootDir(File rootDir) {
		this.rootDir = rootDir;
		if (!rootDir.exists()) {
			throw new RuntimeException("DirectoryCrawler invalid rootDir: " + rootDir.getAbsolutePath());
		}
	}
	
	public Set<TocInfo> generateTOC(Queue<TocInfo> tocQueue) throws Exception {
		Thread loggingThread = new Thread(this);
		
		Set<TocInfo> toc = new HashSet<TocInfo>();
		loggingThread.start();
		
		scanNode(this.rootDir,toc,tocQueue);
		
		this.running = false; // stop logging
		return toc;
	}
	
	public void run() {
		while (running) {
			try {
				logger.info("Generated TOC current size: " + tocInfosGenerated);
				Thread.currentThread().sleep(30000);
				
			} catch(Exception ignore){}
		}
	}
	
	private void scanNode(File node, Set<TocInfo> toc, Queue<TocInfo> tocQueue) throws Exception {
	
		try {
		
			if (node.exists() && !node.getName().startsWith(".")) {
				
				String adjustedPath = node.getAbsolutePath().replace(this.rootDir.getAbsolutePath(), "");
				TocInfo finfo = new TocInfo(adjustedPath, (node.isFile() ? node.length() : 0));
				finfo.setIsDirectory(node.isDirectory());
				toc.add(finfo);
				tocQueue.add(finfo);
				tocInfosGenerated++; // increment for logging
			}
			
			if (node.exists() && !node.getName().startsWith(".") && node.isDirectory()) {
				for (File n : node.listFiles()) {
					scanNode(n,toc,tocQueue);
				}
			}
			
		} catch(Throwable e) {
			logger.error("Permission issue? scanNode(node:"+(node != null? (" path:"+node.getAbsolutePath() +" isDirectory():"+node.isDirectory() + " exists:"+node.exists()): "NULL") + 
								  " toc: " + (toc != null? toc.size(): "NULL") + 
								  " tocQueue:"+(tocQueue != null ? tocQueue.size() : " NULL"));
			
			throw new Exception("scanNode() " + e.getMessage(),e);
		}
	}

}
