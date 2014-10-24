package org.bitsofinfo.s3;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;

public class S3Util {
	
	private static final Logger logger = Logger.getLogger(S3Util.class);
	
	public void uploadToS3(AmazonS3Client s3Client, 
						  String bucketName, 
						  String s3LogBucketFolderRoot, 
						  String host, 
						  List<String> filePathsToUpload) {
		
		try {
			
			for (String file : filePathsToUpload) {
			
				String key = null;
				try {
					File toUpload = new File(file.trim());
					if (!toUpload.exists()) {
						logger.error("uploadToS3() cannot upload file, does not exist! " + toUpload.getAbsolutePath());
					}
					
					key = s3LogBucketFolderRoot + "/" + host + "/" + toUpload.getName();
					 
					PutObjectRequest req = new PutObjectRequest(bucketName, key, toUpload);
					req.setStorageClass(StorageClass.ReducedRedundancy);
					ObjectMetadata objectMetadata = new ObjectMetadata();
					objectMetadata.setContentType("text/plain");
					objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);     
					req.setMetadata(objectMetadata);
					
					s3Client.putObject(req);
					
				} catch(Exception e) {
					logger.error("uploadToS3() unexpected error uploading logs to: " +bucketName + " key:"+ key + " for " +file);
				}
				
			}
			
			
		} catch(Exception e) {
			logger.error("uploadToS3() error uploading logs to S3: " + e.getMessage(),e);
		}
		
	}

}
