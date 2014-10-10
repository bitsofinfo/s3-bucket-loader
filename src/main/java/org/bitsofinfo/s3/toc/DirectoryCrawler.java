package org.bitsofinfo.s3.toc;

import java.io.File;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;


/**
 * Returns all unique directory and file paths
 * 
 * @author inter0p
 *
 */
public class DirectoryCrawler implements SourceTOCGenerator {

	private File rootDir = null;
	
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
		Set<TocInfo> toc = new HashSet<TocInfo>();
		scanNode(this.rootDir,toc,tocQueue);
		return toc;
	}
	
	private void scanNode(File node, Set<TocInfo> toc, Queue<TocInfo> tocQueue) throws Exception {

		if (node.exists() && !node.getName().startsWith(".")) {
			
			String adjustedPath = node.getAbsolutePath().replace(this.rootDir.getAbsolutePath(), "");
			TocInfo finfo = new TocInfo(adjustedPath, (node.isFile() ? node.length() : 0));
			finfo.setIsDirectory(node.isDirectory());
			toc.add(finfo);
			tocQueue.add(finfo);
		}
		
		if (node.isDirectory()) {
			for (File n : node.listFiles()) {
				scanNode(n,toc,tocQueue);
			}
		} 
	}

}
