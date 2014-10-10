package org.bitsofinfo.s3.toc;

import java.io.File;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;


/**
 * Returns all unique directory paths
 * which contain an actual file
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
	
	public Set<FileInfo> generateTOC(Queue<FileInfo> tocQueue) throws Exception {
		Set<FileInfo> toc = new HashSet<FileInfo>();
		scanNode(this.rootDir,toc,tocQueue);
		return toc;
	}
	
	private void scanNode(File node, Set<FileInfo> toc, Queue<FileInfo> tocQueue) throws Exception {

		if (node.exists() && !node.getName().startsWith(".") && node.isFile()) {
			String adjustedPath = node.getAbsolutePath().replace(this.rootDir.getAbsolutePath(), "");
			FileInfo finfo = new FileInfo(adjustedPath,node.length());
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
