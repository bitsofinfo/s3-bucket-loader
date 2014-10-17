package org.bitsofinfo.s3.master;

import java.util.Collection;
import java.util.Set;

import org.bitsofinfo.s3.toc.TocInfo;

public interface TOCGenerationEventHandler {

	public void tocGenerationComplete(Collection<TocInfo> generatedTOC);
	
	public void tocGenerationError(String msg, Exception exception);
}
