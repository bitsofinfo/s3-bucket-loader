package org.bitsofinfo.s3;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.master.Master;
import org.bitsofinfo.s3.worker.Worker;

public class S3BucketLoader {
	
	private static final Logger logger = Logger.getLogger(S3BucketLoader.class);
	
	private static Master master = null;
	private static Worker worker = null;

	public static void main(String[] args) throws Exception {
		
		try {
		
			Properties props = new Properties();
			String confPath = System.getProperty("configFilePath");
			
			InputStream input = null;
			try {
				logger.info("Attempting to load props from: " + confPath);
				input = new FileInputStream(confPath);
				props.load(input);
			} catch(Exception e) {
				e.printStackTrace();
				throw e;
			}
					
			boolean isMaster = Boolean.valueOf(System.getProperty("isMaster"));
			
			Runtime.getRuntime().addShutdownHook(new Thread(new S3BucketLoader().new ShutdownHook()));

			if (isMaster) {
				execAsMaster(props);
			} else {
				execAsWorker(props);
			}
			
			// run until we are shutdown....
			while(true) {
				Thread.currentThread().sleep(60000);
			}
			
		} catch(Exception e) {
			logger.error("main() unexpected error: " + e.getMessage(),e);
		}
		

	}
	
	public class ShutdownHook implements Runnable {
		public void run() {
			try {
				logger.debug("ShutdownHook invoked...");
				if (worker != null) {worker.destroy();}
				if (master != null) {master.destroy();}
			} catch(Exception ignore){}
		}
	}
	
	
	private static void execAsMaster(Properties props) throws Exception {
		master = new Master(props);
		master.start();
	}
	
	public static void execAsWorker(Properties props) {
		worker = new Worker(props);
		worker.startConsuming();
	}


}
