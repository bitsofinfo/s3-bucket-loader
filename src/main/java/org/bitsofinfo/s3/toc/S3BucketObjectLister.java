package org.bitsofinfo.s3.toc;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3BucketObjectLister implements Runnable, SourceTOCGenerator, ProgressListener {
	
	private static final Logger logger = Logger.getLogger(S3BucketObjectLister.class);
	
	private AmazonS3Client s3Client = null;
	private String s3BucketName = null;
	private boolean running = true;
	private int tocInfosGenerated = 0;
	

	public Set<TocInfo> generateTOC(Queue<TocInfo> tocQueue) throws Exception {
		Thread loggingThread = new Thread(this);
		
		Set<TocInfo> toc = new HashSet<TocInfo>();
		loggingThread.start();
		
		scanBucket(toc,tocQueue);
		
		this.running = false; // stop logging
		return toc;
	}
	
	private void scanBucket(Set<TocInfo> toc, Queue<TocInfo> tocQueue) throws Exception {
		
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(s3BucketName);
		// listRequest.setGeneralProgressListener(this);
		listRequest.setMaxKeys(1000);
		
		String nextMarker = null;
		ObjectListing objectListing = null;
		
		while(true) {
			
			objectListing = s3Client.listObjects(listRequest);
			
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			
			for (S3ObjectSummary objSummary : objectSummaries) {
				String key = objSummary.getKey();
				
				TocInfo tocInfo = new TocInfo(key, objSummary.getSize());
				
				// is it a "dir/" ?
				if (key.lastIndexOf("/") == (key.length() - 1)) {
					tocInfo.isDirectory = true;
				} else {
					tocInfo.isDirectory = false;
				}
				
				toc.add(tocInfo);
				tocQueue.add(tocInfo);
				tocInfosGenerated++; // increment for logging
	
			}
			
			// for pagination
			nextMarker = objectListing.getNextMarker();
			if (nextMarker == null) {
				break;
			} else {
				listRequest.setMarker(nextMarker);
				logger.debug("scanBucket() nextMarker we will request listing for => " + nextMarker);
			}
		}
		
	}



	@Override
	public void progressChanged(ProgressEvent progressEvent) {
		logger.debug("progressChanged() " +progressEvent.getEventType() + 
				       " bytes:" + progressEvent.getBytes() + 
				      " bytesTransferred: " + progressEvent.getBytesTransferred());
	}
	
	public void run() {
		while (running) {
			try {
				logger.info("\nGenerated TOC current size: " + tocInfosGenerated + "\n");
				Thread.currentThread().sleep(15000);
				
			} catch(Exception ignore){}
		}
	}

	public AmazonS3Client getS3Client() {
		return s3Client;
	}

	public void setS3Client(AmazonS3Client s3Client) {
		this.s3Client = s3Client;
	}

	public String getS3BucketName() {
		return s3BucketName;
	}

	public void setS3BucketName(String s3BucketName) {
		this.s3BucketName = s3BucketName;
	}


}
