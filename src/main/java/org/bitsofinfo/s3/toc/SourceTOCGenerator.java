package org.bitsofinfo.s3.toc;

import java.util.Queue;
import java.util.Set;

public interface SourceTOCGenerator {

	/**
	 * Generate the TOC list. Implementor should write live/realtime
	 * to the passed tocQueue as well as return a static Set when complete.
	 * 
	 * @param tocQueue
	 * @return
	 * @throws Exception
	 */
	public Set<TocInfo> generateTOC(Queue<TocInfo> tocQueue) throws Exception;
	
}
