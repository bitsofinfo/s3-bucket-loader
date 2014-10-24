package org.bitsofinfo.s3.toc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Reads a TOC file that lists paths relative from root
 * dir and uses that to create TOCInfo objects (after determining the filesize on disk)
 *
 */
public class TOCManifestBasedGenerator implements SourceTOCGenerator, Runnable {

	private static final Logger logger = Logger.getLogger(DirectoryCrawler.class);

	private File rootDir = null;
	private File manifestFile = null;
	private boolean running = true;
	private int tocInfosGenerated = 0;
	
	public TOCManifestBasedGenerator() {}
	
	public TOCManifestBasedGenerator(File rootDir) {
		this.rootDir = rootDir;
	}
	
	public void setRootDir(File rootDir) {
		this.rootDir = rootDir;
		if (!rootDir.exists()) {
			throw new RuntimeException("TOCManifestBasedGenerator invalid rootDir: " + rootDir.getAbsolutePath());
		}
	}
	
	public void setManifestFile(File manifestFile) {
		this.manifestFile = manifestFile;
		if (!manifestFile.exists()) {
			throw new RuntimeException("TOCManifestBasedGenerator invalid manifestFile: " + manifestFile.getAbsolutePath());
		}
	}
	
	public Set<TocInfo> generateTOC(Queue<TocInfo> tocQueue) throws Exception {
		Thread loggingThread = new Thread(this);
		
		Set<TocInfo> toc = new HashSet<TocInfo>();
		loggingThread.start();
		
		BufferedReader reader = new BufferedReader(new FileReader(manifestFile));
		String line = null;
		while ((line = reader.readLine()) != null) {
			File tocEntry = new File(rootDir.getAbsolutePath() + line.trim());
			
			if (tocEntry.exists()) {
				
				String adjustedPath = tocEntry.getAbsolutePath().replace(this.rootDir.getAbsolutePath(), "");
				TocInfo finfo = new TocInfo(adjustedPath, (tocEntry.isFile() ? tocEntry.length() : 0));
				finfo.setIsDirectory(tocEntry.isDirectory());
				toc.add(finfo);
				tocQueue.add(finfo);
				tocInfosGenerated++; // increment for logging
				
			} else {
				logger.warn("generateTOC() file referenced in manifest file: " + tocEntry.getAbsolutePath() + " does not exist!");
			}
		}

		reader.close();
		
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

}
